package booking.analytics

import akka.Done
import akka.actor.typed.ActorSystem
import akka.kafka.{CommitterSettings, ConsumerSettings, Subscriptions}
import akka.kafka.scaladsl.{Committer, Consumer}
import akka.stream.RestartSettings
import akka.stream.scaladsl.RestartSource
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.slf4j.LoggerFactory
import com.google.protobuf.any.{Any => ScalaPBAny}
import booking.proto

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

object ClassRegisterEventConsumer {

  private val log =
    LoggerFactory.getLogger("booking.analytics.ClassRegisterEventConsumer")

  def init(system: ActorSystem[_]): Unit = {
    implicit val sys: ActorSystem[_] = system
    implicit val ec: ExecutionContext =
      system.executionContext

    val topic = system.settings.config
      .getString("booking-analytics-service.booking-kafka-topic")
    val consumerSettings =
      ConsumerSettings(
        system,
        new StringDeserializer,
        new ByteArrayDeserializer).withGroupId("class-booking-analytics")
    val committerSettings = CommitterSettings(system)

    RestartSource
      .onFailuresWithBackoff(
        RestartSettings(
          minBackoff = 1.second,
          maxBackoff = 30.seconds,
          randomFactor = 0.1)) { () =>
        Consumer
          .committableSource(
            consumerSettings,
            Subscriptions.topics(topic)
          )
          .mapAsync(1) { msg =>
            handleRecord(msg.record).map(_ => msg.committableOffset)
          }
          .via(Committer.flow(committerSettings))
      }
      .run()
  }

  private def handleRecord(
                            record: ConsumerRecord[String, Array[Byte]]): Future[Done] = {
    val bytes = record.value()
    val x = ScalaPBAny.parseFrom(bytes)
    val typeUrl = x.typeUrl
    try {
      val inputBytes = x.value.newCodedInput()
      val event =
        typeUrl match {
          case "class-booking-service/ParticipantAdded" =>
            proto.ParticipantAdded.parseFrom(inputBytes)

          case "class-booking-service/ParticipantRemoved" =>
            proto.ParticipantRemoved.parseFrom(inputBytes)

          case "class-booking-service/ClosedClass" =>
            proto.ClosedClass.parseFrom(inputBytes)

          case _ =>
            throw new IllegalArgumentException(
              s"unknown record type [$typeUrl]")
        }

      event match {
        case proto.ParticipantAdded(classId, name, _) =>
          log.info("ParticipantAdded: {} to class {}", name, classId)

        case proto.ParticipantRemoved(classId, name, _) =>
          log.info(
            "ParticipantRemoved: {} to class {}", name, classId)

        case proto.ClosedClass(classId, _) =>
          log.info("ClosedClass: class {} closed", classId)
      }

      Future.successful(Done)
    } catch {
      case NonFatal(e) =>
        log.error("Could not process event of type [{}]", typeUrl, e)
        // continue with next
        Future.successful(Done)
    }
  }
}
