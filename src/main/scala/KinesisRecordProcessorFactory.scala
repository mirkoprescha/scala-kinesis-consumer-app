package de.is24.data.dwh.metricaggregator

import com.amazonaws.services.kinesis.clientlibrary.interfaces.{IRecordProcessor, IRecordProcessorFactory}
import com.typesafe.scalalogging.Logger


class KinesisRecordProcessorFactory (implicit logger:Logger) extends IRecordProcessorFactory {
  override def createProcessor(): IRecordProcessor = {
    new KinesisRecordProcessor()
  }
}
