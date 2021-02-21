package com.zzl.networkflow

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.util.Random


object PageViewUpgrade {
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
      .map(new MyMapper())
       .keyBy(_._1)
      // 一小时滚动窗口
      .timeWindow(Time.hours(1))
      .aggregate(new PvCountAgg(), new PvCountWindowResult())

    val totalPvStream = pvStream
      .keyBy(_.windowEnd)
      .process(new TotalPvCountResult())

    totalPvStream.print()

    env.execute("pv job")

  }
}

// 自定义Mapper，随机生成分组的key
class MyMapper() extends MapFunction[UserBehavior, (String, Long)] {
  override def map(t: UserBehavior): (String, Long) = {
    (Random.nextString(10), 1L)
  }
}

class TotalPvCountResult() extends KeyedProcessFunction[Long, PvCount, PvCount] {
  // 定义一个状态，保存当前所有count总和
  lazy val totalPvCountResultState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("total-pv", classOf[Long]))

  override def processElement(value: PvCount, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#Context, collector: Collector[PvCount]): Unit = {
    // 每来一个数据，将count值叠加在当前的状态下
    val currentTotalCount = totalPvCountResultState.value()
    totalPvCountResultState.update(currentTotalCount + value.count)
    // 注册一个windowEnd+1ms后触发的定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#OnTimerContext, out: Collector[PvCount]): Unit = {
    val totalPvCount = totalPvCountResultState.value()
    out.collect(PvCount(ctx.getCurrentKey, totalPvCount))
    totalPvCountResultState.clear()
  }
}


