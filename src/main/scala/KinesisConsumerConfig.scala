package de.is24.data.dwh.metricaggregator

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger


class KinesisConsumerConfig (implicit logger: Logger) {


  private val conf   = ConfigFactory.load()
  val streamEndpoint = conf.getString("kinesis.inputstream.endpoint")
  val streamName = conf.getString("kinesis.inputstream.name")
  val applicationName = conf.getString("kinesis.application.name")


  logger.info ("KinesisInputStream: " + streamName)
  logger.info ("KinesisApplicationName: " + applicationName)

}

