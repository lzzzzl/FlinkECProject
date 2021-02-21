package com.zzl.networkflow

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

import java.lang

object UvWithBloom {
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

    val uvStream = dataStream
      .filter(_.behavior == "pv")
      .map(data => ("uv", data.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      // 自定义触发器
      .trigger(new MyTrigger())
      .process(new UvCountWithBloom())

    uvStream.print()

    env.execute("uv with bloom job")
  }
}

// 触发器，每来一条数据，直接触发窗口计算并清空窗口状态
class MyTrigger() extends Trigger[(String, Long), TimeWindow] {
  override def onElement(t: (String, Long), l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE

  override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onEventTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {}
}

// 自定义一个布隆过滤器，主要就是一个位图和hash函数
class Bloom(size: Long) extends Serializable {
  // 默认cap应该是2的整次幂
  private val cap = size

  // hash函数
  def hash(value: String, seed: Int): Long = {
    var result = 0
    for (i <- 0 until value.length) {
      result = result * seed + value.charAt(i)
    }
    // 返回hash值，要映射到cap范围内
    (cap - 1) & result
  }
}

class UvCountWithBloom() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow] {
  // 定义redis连接以及布隆过滤器
  lazy val jedis = new Jedis("localhost", 6379)
  // 位的个数：2^6(64) * 2^20(1M) * 2^3(8bit)，64MB
  lazy val bloomFilter = new Bloom(1<<29)

  // 本来是收集齐所有数据，窗口触发计算的时候才会调用；现在每来一条数据都会调用一次
  override def process(key: String, context: ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow]#Context, iterable: lang.Iterable[(String, Long)], collector: Collector[UvCount]): Unit = {
    // TODO
  }
}