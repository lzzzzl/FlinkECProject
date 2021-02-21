package com.zzl.networkflow

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

// 定义输入数据样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)
// 定义输出pv统计的样例类
case class PvCount(windowEnd: Long, count: Long)

object PageView {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val inputStream = env.readTextFile("/Users/zhengzonglin/IdeaProjects/FlinkDemo/NetworkFlowAnalysis/src/main/resources/UserBehavior.csv")

    // 转换成样例类类型并提取时间戳和watermark
    val dataStream: DataStream[UserBehavior] = inputStream
      .map(data => {
        val arr = data.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    val pvStream = dataStream
      .filter(_.behavior == "pv")
      // 定义一个pv字符串作为分组的dummy key
      .map(data => ("pv",1L))
      // 所有数据分到同一组
      .keyBy(_._1)
      // 一小时滚动窗口
      .timeWindow(Time.hours(1))
      .aggregate(new PvCountAgg(), new PvCountWindowResult())

    pvStream.print()

    env.execute("pv job")

  }
}

// 自定义预聚合函数
class PvCountAgg() extends AggregateFunction[(String, Long), Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: (String, Long), acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

// 自定义窗口函数
class PvCountWindowResult() extends WindowFunction[Long, PvCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PvCount]): Unit = {
     out.collect(PvCount(window.getEnd, input.head))
  }
}

