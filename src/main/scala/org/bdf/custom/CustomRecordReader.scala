package org.bdf.custom

import java.io.{BufferedReader, InputStreamReader}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext, JobContext}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat


class CustomRecordReader extends RecordReader[LongWritable, Text] {

  private var reader: BufferedReader = _
  private var key: LongWritable = _
  private var value: Text = _

  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
    val fileSplit = split.asInstanceOf[FileSplit]
    reader = new BufferedReader(new InputStreamReader(fileSplit.getPath.getFileSystem(context.getConfiguration).open(fileSplit.getPath)))
  }

  override def nextKeyValue(): Boolean = {
    var line: String = reader.readLine()
    val sb = new StringBuilder()

    while (line != null) {
      if (line.startsWith("COPY")) {
        key = new LongWritable(line.hashCode.toLong)
      } else if (line.equals(".\\")) {
        value = new Text(sb.toString())
        return true
      } else {
        sb.append(line).append("\n")
      }
      line = reader.readLine()
    }

    false
  }

  override def getCurrentKey: LongWritable = key

  override def getCurrentValue: Text = value

  override def getProgress: Float = 0.0f

  override def close(): Unit = reader.close()
}
