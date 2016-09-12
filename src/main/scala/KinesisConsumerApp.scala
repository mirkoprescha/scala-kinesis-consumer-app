package de.is24.data.dwh.metricaggregator

import java.net.InetAddress
import java.util.UUID

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{KinesisClientLibConfiguration, Worker}
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

object KinesisConsumerApp extends App{
  System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "TRACE")

  implicit val logger = Logger(LoggerFactory.getLogger("KinesisConsumerApp"))
  logger.info ("start reading App Config")
  val kinesisConsumerConfig = new KinesisConsumerConfig


  val workerId = InetAddress.getLocalHost().getCanonicalHostName() +":" + UUID.randomUUID()

  val kclConfig:KinesisClientLibConfiguration = new KinesisClientLibConfiguration(
    kinesisConsumerConfig.applicationName,
    kinesisConsumerConfig.streamName,
    new ProfileCredentialsProvider(),
    workerId
  ).withKinesisEndpoint(kinesisConsumerConfig.streamEndpoint)
    .withRegionName(kinesisConsumerConfig.dynamoDBRegionName)

  logger.info (s"Created KinesisClientLibConfiguration for Application ${kinesisConsumerConfig.applicationName} consuming ${kinesisConsumerConfig.streamName}")

  val recordProcessorFactory = new KinesisRecordProcessorFactory()
  val worker = new Worker(
    recordProcessorFactory,
    kclConfig,
    new NullMetricsFactory()
  )
  logger.info (s"Created Worker for KinesisClientLibConfiguration with workerId ${workerId}")

  worker.run()

}
