package org.bdf.custom

import org.apache.hadoop.io.{LongWritable, Text}

import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext, JobContext}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat


class CustomInputFormat extends FileInputFormat[LongWritable, Text] {

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[LongWritable, Text] = {
    new CustomRecordReader
  }

}
