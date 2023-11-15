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

  val endTag1 = "COPY".getBytes();
  val endTag2 = "\\.".getBytes();

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
      readUntilMatch(endTag1, endTag2, false);
    }
  }

  @throws(classOf[IOException])
  override def nextKeyValue(): Boolean = {
    if (!stillInChunk) return false;

    val status = readUntilMatch(endTag1, endTag2, true);

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
  def readUntilMatch(match1: Array[Byte], match2: Array[Byte], withinBlock: Boolean): Boolean = {
    var i = 0;
    var j = 0;
    while (true) {
      val b = fsin.read();
      if (b == -1) return false;

      if (b == match1(i)) {
        tempBuffer1.+=(b)
        i = i + 1;
        if (i >= match1.length) {
          tempBuffer1.clear()
          return fsin.getPos() < end;
        }
      } else if (b == match2(j)) {
        tempBuffer2.+=(b)
        j = j + 1;
        if (j >= match2.length) {
          tempBuffer2.clear()
          return fsin.getPos() < end;
        }
      } else {
        if (tempBuffer1.size != 0)
          tempBuffer1.foreach { x => if (withinBlock) buffer.write(x) }
        else if (tempBuffer2.size != 0)
          tempBuffer2.foreach { x => if (withinBlock) buffer.write(x) }
        tempBuffer1.clear()
        tempBuffer2.clear()
        if (withinBlock) buffer.write(b);
        i = 0;
        j = 0;
      }
    }
    return false;
  }
}
