package de.is24.data.dwh.metricaggregator

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

object KinesisConsumerApp extends App{
  System.setProperty("org.slf4j.simplelogger.defaultlog", "trace")
  implicit val log = Logger(LoggerFactory.getLogger("KinesisConsumerApp"))
  log.info ("!!!!!create consumer")
  val consumerConfig = new KinesisConsumerConfig


}
