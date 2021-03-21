package cz.jenda.alarm.garage

import pureconfig.generic.ProductHint
import pureconfig.generic.auto._
import pureconfig.{CamelCase, ConfigFieldMapping, ConfigSource}

import scala.concurrent.duration.FiniteDuration

case class AppConfiguration(mqtt: MqttConfiguration)

object AppConfiguration {
  private implicit def hint[A]: ProductHint[A] = ProductHint[A](ConfigFieldMapping(CamelCase, CamelCase))

  def load: AppConfiguration = {
    ConfigSource.default.at("app").loadOrThrow[AppConfiguration]
  }
}

case class MqttConfiguration(
    host: String,
    port: Int,
    ssl: Boolean,
    user: Option[String],
    pass: Option[String],
    topic: String,
    subscriberName: String,
    readTimeout: FiniteDuration,
    connectionRetries: Int,
    keepAliveSecs: Int
)
