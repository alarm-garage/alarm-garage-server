package cz.jenda.alarm.garage

import cats.effect.ExitCode
import monix.eval.{Task, TaskApp}
import net.sigusr.mqtt.api.Message
import protocol.alarm.{Report, State}
import slog4s.slf4j.Slf4jFactory

import java.time.{Duration, LocalDateTime, ZoneOffset}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.util.control.NonFatal

object Main extends TaskApp {
  private lazy val lastState: AtomicReference[Option[State]] = new AtomicReference(None)

  private val totalTime = new AtomicReference[Double](0)
  private val totalTimeCount = new AtomicInteger()
  private val totalEventsCount = new AtomicInteger()

  override def run(args: List[String]): Task[ExitCode] = {
    val config = AppConfiguration.load

    val loggerFactory = Slf4jFactory[Task].withoutContext.loggerFactory

    val program = for {
      sub <- MqttModule.make(config.mqtt, loggerFactory.make("MqttSubscription"), processMessage)
    } yield {
      sub
    }

    program.use { sub =>
      sub.connectAndAwait *> Task.never[ExitCode]
    }
  }

  def processMessage(m: Message): Task[Unit] = {
    Task {
      val bytes = m.payload.toArray
      val report = Report.parseFrom(bytes)

      lastState.set(Some(report.state))

      val time = report.timestamp.map { t =>
        LocalDateTime.ofEpochSecond(t / 1000, ((t % 1000) * 1000000).toInt, ZoneOffset.ofHours(1))
      }

      val now = LocalDateTime.now()
      val diff = time.map(Duration.between(_, now))

      if (totalEventsCount.getAndIncrement() > 0) {
        diff.foreach { d =>
          totalTime.updateAndGet((t: Double) => t + d.toMillis.toDouble.abs / 1000)
          totalTimeCount.incrementAndGet()
        }
      }

      println(s"$now/$time/$diff: ${report.state}")

      if (totalTimeCount.get() > 0 && totalTimeCount.get() % 20 == 0) {
        println(s"AVG diff: ${totalTime.get() / totalTimeCount.get()}s")
      }
    }.onErrorRecover {
      case NonFatal(e) =>
        e.printStackTrace()
    } *> Task.unit
  }
}
