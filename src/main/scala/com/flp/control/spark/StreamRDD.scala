package com.flp.control.spark

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.{TaskContext, Partition, SparkContext, Logging}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

private[spark] class StreamPartition(
  val index: Int
) extends Partition

class StreamRDD[T: ClassTag](
  sc: SparkContext,
  streams: Seq[Stream[T]]
) extends RDD[T](sc, Nil) with Logging {

  override protected def getPartitions: Array[Partition] =
    (0 until streams.length).map {
      i => new StreamPartition(i)
    }.toArray[Partition]

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[T] =
    streams(split.asInstanceOf[StreamPartition].index).iterator

}
