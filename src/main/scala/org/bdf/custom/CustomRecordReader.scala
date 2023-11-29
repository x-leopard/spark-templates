package org.bdf.custom

import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, TextInputFormat}
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hadoop.io.DataOutputBuffer

import java.io.IOException
import scala.collection.mutable.MutableList



class CustomInputFormat extends FileInputFormat[LongWritable, Text] {

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[LongWritable, Text] = {
    new CustomRecordReader
  }

}

class CustomRecordReader extends RecordReader[LongWritable, Text] {
  private var end: Long = _
  private var stillInChunk = true
  private val key = new LongWritable()
  private val value = new Text()
  private var fsin: FSDataInputStream = _
  private val buffer = new DataOutputBuffer()
  private val endTag = "\\.".getBytes()

  override def initialize(inputSplit: InputSplit, taskAttemptContext: TaskAttemptContext): Unit = {
    val split = inputSplit.asInstanceOf[FileSplit]
    val conf = taskAttemptContext.getConfiguration
    val path = split.getPath
    val fs = path.getFileSystem(conf)
    fsin = fs.open(path)
    val start = split.getStart
    end = split.getStart + split.getLength
    fsin.seek(start)
    if (start != 0) {
      readUntilMatch(endTag, false)
    }
  }

  override def nextKeyValue(): Boolean = {
    if (!stillInChunk) {
      return false
  }
    val status = readUntilMatch(endTag, true)
    value.set(buffer.getData, 0, buffer.getLength)
    key.set(fsin.getPos)
    buffer.reset()
    if (!status) {
      stillInChunk = false
    }
    true
  }

  override def getCurrentKey: LongWritable = key

  override def getCurrentValue: Text = value

  override def getProgress: Float = 0

  override def close(): Unit = fsin.close()

  private def readUntilMatch(matcher: Array[Byte], withinBlock: Boolean): Boolean = {
    var i = 0
    while (true) {
      val b = fsin.read()
      if (b == -1) return false
      if (withinBlock) buffer.write(b)
      if (b == matcher(i)) {
        i += 1
        if (i >= matcher.length) {
          return fsin.getPos < end
        }
      } else i = 0
    }
    false
  }
}


object Test {
  def main(args: Array[String]): Unit = {
    println("Hello!")
    val conf = new Configuration()
    val path = new Path("file:////workspaces/spark-templates/src/main/scala/org/bdf/custom/text.txt")
    val reader = new CustomRecordReader()
    val fileSplit = new FileSplit(path, 0, 400, Array.empty)
    println(s"[Custom log]    [Custom log]    fileSplit: $fileSplit")
    val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
    val hadoopAttemptContext = new TaskAttemptContextImpl(conf, attemptId)
    reader.initialize(fileSplit, hadoopAttemptContext)
    while (reader.nextKeyValue()) {
      val key = reader.getCurrentKey
      val value = reader.getCurrentValue
      println(s"[Custom log]    $key: $value")
    }
    reader.close()
  }
}

