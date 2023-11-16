package org.bdf.custom
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, TextInputFormat }
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import scala.io.Source
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.io.DataOutputBuffer

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.fs.FSDataInputStream
import scala.collection.mutable.MutableList
import java.io.IOException



class ParagraphRecordReader extends RecordReader[LongWritable, Text] {
  var end: Long = 0L;
  var stillInChunk = true;

  var key = new LongWritable();
  var value = new Text();

  var fsin: FSDataInputStream = null;
  val buffer = new DataOutputBuffer();
  val tempBuffer1 = MutableList[Int]();
  val tempBuffer2 = MutableList[Int]();

  val endTag = "\\.".getBytes();

  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  override def initialize(inputSplit: org.apache.hadoop.mapreduce.InputSplit, taskAttemptContext: org.apache.hadoop.mapreduce.TaskAttemptContext) : Unit = {
    val split = inputSplit.asInstanceOf[FileSplit];
    val conf = taskAttemptContext.getConfiguration();
    val path = split.getPath();
    val fs = path.getFileSystem(conf);

    fsin = fs.open(path);
    val start = split.getStart();
    end = split.getStart() + split.getLength();
    fsin.seek(start);

    if (start != 0) {
      readUntilMatch(endTag, false);
    }
  }

  @throws(classOf[IOException])
  override def nextKeyValue(): Boolean = {
    if (!stillInChunk) return false;

    val status = readUntilMatch(endTag, true);

    value = new Text();
    value.set(buffer.getData(), 0, buffer.getLength());
    key = new LongWritable(fsin.getPos());
    buffer.reset();

    if (!status) {
      stillInChunk = false;
    }

    return true;
  }

  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  override def getCurrentKey(): LongWritable = {
    return key;

  }

  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  override def getCurrentValue(): Text = {
    return value;
  }

  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  override def getProgress(): Float = {
    return 0;
  }

  @throws(classOf[IOException])
  override def close() : Unit = {
    fsin.close();
  }

  @throws(classOf[IOException])
  def readUntilMatch(matched: Array[Byte], withinBlock: Boolean): Boolean = {
    var i = 0;
    while (true) {
      val b = fsin.read();
      if (b == -1) return false;
      if (withinBlock) buffer.write(b);
      if (b == matched(i)) {
        i = i + 1;
        if (i >= matched.length) {
          return fsin.getPos() < end;
        }
      } 
      else i = 0;
    }
    return false;
  }
}
