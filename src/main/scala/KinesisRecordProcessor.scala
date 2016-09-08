import java.util

import com.amazonaws.services.kinesis.clientlibrary.interfaces.{IRecordProcessor, IRecordProcessorCheckpointer}
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason
import com.amazonaws.services.kinesis.model
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


  override def processRecords(records: util.List[model.Record], checkpointer: IRecordProcessorCheckpointer): Unit = {
    for (records) {
      try {
        logger.debug(s"Processing ${records} records from ${kinesisShardId}")
        logger.trace(s"Sequence number: ${record.getSequenceNumber}")
        logger.trace(record.getData.array)
        logger.trace(s"Partition key: ${record.getPartitionKey}")
      } catch {
        case t: Throwable =>
          println(s"Caught throwable while processing record $record")
          println(t)
      }
    }


  override def shutdown(checkpointer: IRecordProcessorCheckpointer, reason: ShutdownReason): Unit = {
    logger.info (s"Shutting down record processor for shard: $kinesisShardId")
    if (reason == ShutdownReason.TERMINATE) {
      ///checkpoint(checkpointer)
      println("shutdown")
    }
  }

//
//  private def checkpoint(checkpointer: IRecordProcessorCheckpointer) = {
//    println(s"Checkpointing shard $kinesisShardId")
//    breakable { for (i <- 0 to NUM_RETRIES-1) {
//      try {
//        checkpointer.checkpoint()
//        break
//      } catch {
//        case se: ShutdownException =>
//          println("Caught shutdown exception, skipping checkpoint.", se)
//        case e: ThrottlingException =>
//          if (i >= (NUM_RETRIES - 1)) {
//            println(s"Checkpoint failed after ${i+1} attempts.", e)
//          } else {
//            println(s"Transient issue when checkpointing - attempt ${i+1} of "
//              + NUM_RETRIES, e)
//          }
//        case e: InvalidStateException =>
//          println("Cannot save checkpoint to the DynamoDB table used by " +
//            "the Amazon Kinesis Client Library.", e)
//      }
//      Thread.sleep(BACKOFF_TIME_IN_MILLIS)
//    }
//    } }
}
