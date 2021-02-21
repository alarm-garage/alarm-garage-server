package cz.jenda.alarm.garage

import pureconfig.generic.ProductHint
import pureconfig.generic.auto._
import pureconfig.{CamelCase, ConfigFieldMapping, ConfigSource}

case class AppConfiguration(mqtt: MqttConfiguration)

object AppConfiguration {
  private implicit def hint[A]: ProductHint[A] = ProductHint[A](ConfigFieldMapping(CamelCase, CamelCase))

  def load: AppConfiguration = {
    ConfigSource.default.at("app").loadOrThrow[AppConfiguration]
  }
}

case class MqttConfiguration(host: String, port: Int, topic: String, subscriberName: String)
