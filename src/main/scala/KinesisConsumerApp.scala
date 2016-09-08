package de.is24.data.dwh.metricaggregator

import java.net.InetAddress
import java.util.UUID

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{Worker, KinesisClientLibConfiguration}
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

object KinesisConsumerApp extends App{
  System.setProperty("org.slf4j.simplelogger.defaultlog", "trace")
  implicit val logger = Logger(LoggerFactory.getLogger("KinesisConsumerApp"))
  logger.info ("start reading App Config")
  val consumerConfig = new KinesisConsumerConfig


  val workerId = InetAddress.getLocalHost().getCanonicalHostName() +":" + UUID.randomUUID()
  logger.debug (s"Using workerId: ${workerId}")


  val kclConfig:KinesisClientLibConfiguration = new KinesisClientLibConfiguration(
    consumerConfig.applicationName,
    consumerConfig.streamName,
    new ProfileCredentialsProvider(),
    workerId
  )

  val recordProcessorFactory = new KinesisRecordProcessorFactory()
  val worker = new Worker(
    recordProcessorFactory,
    kclConfig,
    new NullMetricsFactory()
  )
  worker.run()

}
