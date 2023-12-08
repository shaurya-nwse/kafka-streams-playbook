package org.hackweek.xing

import io.circe.{Decoder, Encoder}
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.kstream.{GlobalKTable, JoinWindows}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{KStream, KTable}
import org.apache.kafka.streams.scala.serialization.Serdes

import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.Properties

// important
import org.apache.kafka.streams.scala.ImplicitConversions._

object KStreamsWorker {

  object Domain {
    type UserId  = String
    type Profile = String
    type Product = String
    type OrderId = String
    type Status  = String

    case class Order(orderId: OrderId, user: UserId, products: List[Product], amount: Double) // don't use double for money
    case class Discount(profile: Profile, amount: Double)
    case class Payment(orderId: OrderId, status: Status)

  }

  object Topics {
    val OrdersByUser           = "orders-by-user"
    val DiscountProfilesByUser = "discount-profiles-by-user"
    val Discounts              = "discounts"
    val Orders                 = "orders"
    val Payments               = "payments"
    val PaidOrders             = "paid-orders"

    // intermediate topics
    val OrdersWithProfiles = "orders-with-profiles"
  }

  def generateTopicCommands(topics: List[String]): Unit = {
    topics.foreach { topic =>
      if (topic == "discounts" || topic == "discount-profiles-by-user") {
        println(
          s"bin/kafka-topics.sh --zookeeper localhost:2181 --topic ${topic} --create --replication-factor 1 " +
            s"--partitions 1 --config cleanup.policy=compact"
        )
      } else {
        println(
          s"bin/kafka-topics.sh --zookeeper localhost:2181 --topic ${topic} --create --replication-factor 1 " +
            s"--partitions 1"
        )
      }
    }
  }

  // serde to/from arrays of bytes

//  implicit val serdeOrder: Serde[Order] = {
//    val serializer = (order: Order) => order.asJson.noSpaces.getBytes()
//    val deserializer = (bytes: Array[Byte]) => {
//      val string = new String(bytes)
//      decode[Order](string).toOption
//    }
//    Serdes.fromFn[Order](serializer, deserializer)
//  }

  // make serde generic
  implicit def serde[A >: Null: Decoder: Encoder]: Serde[A] = {
    val serializer = (c: A) => c.asJson.noSpaces.getBytes()
    val deserializer = (bytes: Array[Byte]) => {
      val string = new String(bytes)
      decode[A](string).toOption
    }
    Serdes.fromFn[A](serializer, deserializer)
  }

  def main(args: Array[String]): Unit = {
    import Domain._
    import Topics._
    // topology
    val builder = new StreamsBuilder()

    // KStream
    val usersOrdersStream: KStream[UserId, Order] = builder.stream[UserId, Order](OrdersByUser).peek((k, v) => println(s"Key: $k, value: $v"))

    // KTable: distributed between nodes
    val userProfilesTable: KTable[UserId, Profile] = builder.table[UserId, Profile](DiscountProfilesByUser)

    // Global KTable: copied to all the nodes in the cluster (should store few values)
    val discountProfilesGTable: GlobalKTable[Profile, Discount] = builder.globalTable[Profile, Discount](Discounts)

    // transformation on KStream gives another KStream
    val expensiveOrders: KStream[UserId, Order] = usersOrdersStream
      .filter { (userId, order) =>
        order.amount > 1000
      }

    // filter, map, mapValues, flatMap, flatMapValues
    val listsOfProducts: KStream[UserId, List[Product]] = usersOrdersStream.mapValues { order =>
      order.products
    }

    val productsStream: KStream[UserId, Product] = usersOrdersStream.flatMapValues(_.products)

    // joins
    val ordersWithUserProfiles: KStream[UserId, (Order, Profile)] = usersOrdersStream
      .join(userProfilesTable) { (order, profile) =>
        println(s"Combing profile with orders: \n Order: $order, Profile: $profile")
        (order, profile)
      }

    val discountedOrdersStream: KStream[UserId, Order] = ordersWithUserProfiles
      .join(discountProfilesGTable)(
        // key to join; picked from the "left" stream
        { case (userId, (order, profile)) => profile },
        // (value from stream, value from GTable) => returned value
        {
          case ((order, profile), discount) =>
            println(s"Computing order after discount with order: $order, discount: $discount")
            order.copy(amount = order.amount - discount.amount)
        }
      )

    // pick another identifier; change key
    val ordersStream: KStream[OrderId, Order] = discountedOrdersStream.selectKey((userId, order) => order.orderId)
    // which orders were paid for?
    val paymentsStream: KStream[OrderId, Payment] = builder.stream[OrderId, Payment](Payments)
    // stream joins need join window
    val joinWindow: JoinWindows = JoinWindows.of(Duration.of(5, ChronoUnit.MINUTES))

    val joinOrdersPayments: (Order, Payment) => Option[Order] = (order: Order, payment: Payment) =>
      if (payment.status == "PAID") Option(order) else Option.empty[Order]
    val ordersPaid: KStream[OrderId, Order] = ordersStream
      .join(paymentsStream)(joinOrdersPayments, joinWindow)
      .flatMapValues(maybeOrder => maybeOrder.toList)

    // sink
    ordersPaid.to(PaidOrders)

    val topology = builder.build()
    val props    = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-application-2")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)

//    println(topology.describe())

    val application = new KafkaStreams(topology, props)
    application.start()
  }
}
