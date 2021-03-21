package cz.jenda.alarm.garage

import cats.effect.Resource
import cats.effect.concurrent.Ref
import cats.syntax.all._
import monix.eval.Task
import net.sigusr.mqtt.api.ConnectionState.{Connected, Connecting, Disconnected, Error, SessionStarted}
import net.sigusr.mqtt.api.Errors.{ConnectionFailure, ProtocolError}
import net.sigusr.mqtt.api.QualityOfService.AtLeastOnce
import net.sigusr.mqtt.api.RetryConfig.Custom
import net.sigusr.mqtt.api.{ConnectionState, _}
import retry.RetryPolicies
import slog4s.Logger

import scala.concurrent.duration._

object MqttModule {
  trait Subscription {
    def connectAndAwait: Task[Unit]
  }

  def make(
      mqttConfiguration: MqttConfiguration,
      logger: Logger[Task],
      processMessage: Message => Task[Unit]
  ): TaskResource[Subscription] = {
    val retryConfig: Custom[Task] = Custom[Task](
      RetryPolicies
        .limitRetries[Task](mqttConfiguration.connectionRetries)
        .join(RetryPolicies.fullJitter[Task](2.seconds))
    )

    val transportConfig = TransportConfig[Task](
      mqttConfiguration.host,
      mqttConfiguration.port,
      tlsConfig = if (mqttConfiguration.ssl) Some(TLSConfig(TLSContextKind.System)) else None,
      retryConfig = retryConfig,
      traceMessages = false
    )

    val sessionConfig = SessionConfig(
      clientId = mqttConfiguration.subscriberName,
      cleanSession = false,
      user = mqttConfiguration.user,
      password = mqttConfiguration.pass,
      keepAlive = mqttConfiguration.keepAliveSecs
    )

    val topics = Vector(mqttConfiguration.topic -> AtLeastOnce)

    Session[Task](transportConfig, sessionConfig).flatMap { session =>
      Resource.liftF(Ref[Task].of(false).map { started =>
        val sessionStatus = session.state.discrete
          .evalTap(logSessionStatus(logger, started))
          .evalTap(onSessionError)

        val subscription = fs2.Stream.eval(session.subscribe(topics)) *> session.messages().evalMap(processMessage)

        new Subscription {
          override def connectAndAwait: Task[Unit] = sessionStatus.concurrently(subscription).compile.drain
        }
      })
    }
  }

  private def logSessionStatus(logger: Logger[Task], started: Ref[Task, Boolean]): ConnectionState => Task[Unit] = {
    case Error(ConnectionFailure(reason))    => logger.error(reason.show)
    case Error(ProtocolError)                => logger.error("Ṕrotocol error")
    case Disconnected                        => started.get.flatMap(s => if (s) logger.warn("Transport disconnected") else Task.unit)
    case Connecting(nextDelay, retriesSoFar) => logger.warn(s"Transport connecting. $retriesSoFar attempt(s) so far, next in $nextDelay")
    case Connected                           => logger.info("Transport connected")
    case SessionStarted                      => started.set(true) *> logger.info("Session started")
  }

  private def onSessionError: ConnectionState => Task[Unit] = {
    case Error(e) => Task.raiseError(e)
    case _        => Task.pure(())
  }

}
