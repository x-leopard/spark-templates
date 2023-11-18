package org.bdf.custom

import org.apache.hadoop.mapreduce._
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, TextInputFormat}
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import scala.collection.mutable.ListBuffer
import java.io.IOException
import org.apache.hadoop.io.DataOutputBuffer
import scala.collection.mutable.MutableList
import scala.collection.mutable.ArrayBuffer
import java.nio.charset.StandardCharsets

class CopyRecordReader extends RecordReader[LongWritable, Text] {
  private var start: Long = 0L
  private var end: Long = 0L
  private var pos: Long = 0L
  private var fsin: FSDataInputStream = _
  private val buffer: ArrayBuffer[String] = ArrayBuffer.empty[String]

  override def initialize(inputSplit: InputSplit, taskAttemptContext: TaskAttemptContext): Unit = {
    val fileSplit = inputSplit.asInstanceOf[FileSplit]
    val conf = taskAttemptContext.getConfiguration
    val path = fileSplit.getPath
    val fs = path.getFileSystem(conf)
    fsin = fs.open(path)
    start = fileSplit.getStart
    end = start + fileSplit.getLength
    pos = start
  }

  override def nextKeyValue(): Boolean = {
    if (pos < end) {
      var line = fsin.readLine()
      if (line != null) { 
        pos = pos + line.getBytes(StandardCharsets.UTF_8).length + 1
      }
      else { 
        return false
      }
      if (line.contains("COPY")) {
        buffer.clear()
        buffer += line
        while (pos < end && !line.contains("\\.")) {
          line = fsin.readLine()
          pos += line.getBytes(StandardCharsets.UTF_8).length + 1
          buffer += line
        }
        return true
      }
      nextKeyValue()
    } else {
      false
    }
  }

  override def getCurrentKey: LongWritable = new LongWritable(pos)

  override def getCurrentValue: Text = new Text(buffer.mkString("\n"))

  override def getProgress: Float = (pos - start) / (end - start).toFloat

  override def close(): Unit = fsin.close()
}


object Main {
  def main(args: Array[String]): Unit = {
    println("Hello!")
    val conf = new Configuration()
    val path = new Path("file:////workspaces/spark-templates/src/main/scala/org/bdf/custom/text.txt")
    val reader = new CopyRecordReader()
    val fileSplit = new FileSplit(path, 0, 100000, Array.empty)
    val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
    val hadoopAttemptContext = new TaskAttemptContextImpl(conf, attemptId)
    reader.initialize(fileSplit, hadoopAttemptContext)
    while (reader.nextKeyValue()) {
      val key = reader.getCurrentKey
      val value = reader.getCurrentValue
      println(s"$key: $value")
    }
    reader.close()
  }
}

