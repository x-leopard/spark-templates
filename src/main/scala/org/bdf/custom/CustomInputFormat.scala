package org.bdf.custom

import org.apache.hadoop.mapreduce.lib.input.{FileSplit, TextInputFormat }
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import scala.io.Source
import org.apache.hadoop.mapreduce._

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.fs.FSDataInputStream
import scala.collection.mutable.MutableList
import java.io.IOException

class CustomInputFormat extends TextInputFormat {
  override def createRecordReader(inputSplit: InputSplit,  taskAttemptContext: TaskAttemptContext): RecordReader[LongWritable, Text] = {
    return new ParagraphRecordReader();
  }
}