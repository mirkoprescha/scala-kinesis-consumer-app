package de.is24.data.dwh.metricaggregator

import java.util
import com.amazonaws.services.kinesis.clientlibrary.exceptions.{InvalidStateException, ThrottlingException, ShutdownException}

import scala.collection.JavaConversions._
import com.amazonaws.services.kinesis.clientlibrary.interfaces.{IRecordProcessor, IRecordProcessorCheckpointer}
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason
import com.amazonaws.services.kinesis.model.Record
import com.typesafe.scalalogging.Logger

/**
  * Created by mprescha on 07.09.16.
  */

class KinesisRecordProcessor (implicit logger: Logger) extends IRecordProcessor{

  private var kinesisShardId: String = _
  private var nextCheckpointTimeInMillis: Long = _

  // Backoff and retry settings.
  private val BACKOFF_TIME_IN_MILLIS = 3000L
  private val NUM_RETRIES = 10
  private val CHECKPOINT_INTERVAL_MILLIS = 1000L


  override def initialize(shardId: String): Unit = {
    logger.info ("Initializing record processor for shard: " + shardId)
    this.kinesisShardId = shardId
  }


  override def processRecords(records: util.List[Record], checkpointer: IRecordProcessorCheckpointer): Unit = {
    logger.debug(s"Processing ${records.size} records from ${kinesisShardId}")

    for (record <- records) {
      try {
        logger.trace(s"Partition key: ${record.getPartitionKey}")
        logger.trace(s"Sequence number: ${record.getSequenceNumber}")
        logger.trace(s"ApproximateArrivalTimestamp: ${record.getApproximateArrivalTimestamp}")
        logger.trace(s"Record data: ${record.getData.toString}")
     //   checkpoint(checkpointer)

      } catch {
        case t: Throwable =>
          println(s"Caught throwable while processing record $record")
          println(t)
      }
    }
  }


  override def shutdown(checkpointer: IRecordProcessorCheckpointer, reason: ShutdownReason): Unit = {
    logger.info (s"Shutting down record processor for shard: $kinesisShardId")
    if (reason == ShutdownReason.TERMINATE) {
      checkpoint(checkpointer)
      println("shutdown")
    }
  }


  private def checkpoint(checkpointer: IRecordProcessorCheckpointer) = {
    logger.debug(s"Checkpointing shard $kinesisShardId")
    for (i <- 0 to NUM_RETRIES - 1) {
      try {
        checkpointer.checkpoint()
      } catch {
        case se: ShutdownException =>
          logger.error("Caught shutdown exception, skipping checkpoint.", se)
        case e: ThrottlingException =>
          if (i >= (NUM_RETRIES - 1)) {
            logger.error(s"Checkpoint failed after ${i + 1} attempts.", e)
          } else {
            logger.warn(s"Transient issue when checkpointing - attempt ${i + 1} of "
              + NUM_RETRIES, e)
            Thread.sleep(BACKOFF_TIME_IN_MILLIS)
          }
        case e: InvalidStateException =>
          logger.error("Cannot save checkpoint to the DynamoDB table used by " +
            "the Amazon Kinesis Client Library.", e)
      }

    }
  }
}
