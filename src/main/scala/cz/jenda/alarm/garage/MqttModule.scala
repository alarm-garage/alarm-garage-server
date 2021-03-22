package cz.jenda.alarm.garage

import cats.effect.Resource
import cats.syntax.all._
import fs2.concurrent.SignallingRef
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
  def make(
      mqttConfiguration: MqttConfiguration,
      logger: Logger[Task],
      processMessage: Message => Task[Unit]
  ): TaskResource[MqttModule.type] = {
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
      Resource(for {
        _ <- logger.debug(s"Initializing MQTT connection to ${mqttConfiguration.host}:${mqttConfiguration.port}")
        stopSignal <- SignallingRef[Task, Boolean](false)
        startedResult <- SignallingRef[Task, Option[Option[Throwable]]](None) // isStarted[wasError]

        sessionStatusStream =
          session.state.discrete
            .evalTap(logSessionStatus(logger, startedResult))
            .evalTap(onSessionError)

        subscriptionStream = fs2.Stream.eval(session.subscribe(topics)) *> session.messages().evalMap(processMessage)

        waitForStart =
          startedResult.continuous.unNone
            .take(1)
            .flatMap[Task, Unit] {
              case None    => fs2.Stream.empty
              case Some(e) => fs2.Stream.raiseError[Task](e)
            }
            .compile
            .drain

        _ <- sessionStatusStream.concurrently(subscriptionStream).interruptWhen(stopSignal).compile.drain.start
        _ <- waitForStart
      } yield {

        // here we are in Resource; `stopSignal.set(true)` will interrupt all streams above and end the processing
        (MqttModule, logger.debug("Shutting down MQTT connection") *> stopSignal.set(true))
      })
    }
  }

  private def logSessionStatus(
      logger: Logger[Task],
      started: SignallingRef[Task, Option[Option[Throwable]]]
  ): ConnectionState => Task[Unit] = {
    case Error(e @ ConnectionFailure(reason)) => started.set(Some(Some(e))) *> logger.error(reason.show)
    case Error(ProtocolError)                 => logger.error("Ṕrotocol error")
    case Disconnected                         => started.get.flatMap(s => if (s.nonEmpty) logger.warn("Transport disconnected") else Task.unit)
    case Connecting(nextDelay, retriesSoFar)  => logger.warn(s"Transport connecting. $retriesSoFar attempt(s) so far, next in $nextDelay")
    case Connected                            => logger.info("Transport connected")
    case SessionStarted                       => started.set(Some(None)) *> logger.info("Session started")
  }

  private def onSessionError: ConnectionState => Task[Unit] = {
    case Error(e) => Task.raiseError(e)
    case _        => Task.pure(())
  }

}
