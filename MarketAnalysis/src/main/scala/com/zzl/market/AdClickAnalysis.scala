package com.zzl.market

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp

// 定义输入输出样例类
case class AdClickLog(userId: Long, adId: Long, province: String, city: String, timestamp: Long)
case class AdClickCountByProvince(windowEnd: String, province: String, count: Long)
case class BlackListUserWarning(userId: Long, adId: Long, msg: String)

object AdClickAnalysis {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 从文件中读取数据
    val inputStream = env.readTextFile("/Users/zhengzonglin/IdeaProjects/FlinkDemo/MarketAnalysis/src/main/resources/AdClickLog.csv")

    // 转换成样例类
    val adLogStream = inputStream
      .map(data => {
        val arr = data.split(",")
        AdClickLog(arr(0).toLong, arr(1).toLong, arr(2), arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 插入一步过滤操作，并将有刷单行为的用户输出到侧输出流（黑名单报警）
    val filterBlackListUserStream: DataStream[AdClickLog] = adLogStream
      .keyBy(data => (data.userId, data.adId))
      .process(new FilterBlackListUserResult())

    // 开窗聚合统计
    val adCountResultStream = filterBlackListUserStream
      .keyBy(_.province)
      .timeWindow(Time.days(1), Time.seconds(5))
      .aggregate(new AdCountAgg(), new AdCountWindowResult())

    adCountResultStream.print("count result")
    filterBlackListUserStream.getSideOutput(new OutputTag[BlackListUserWarning]("warning")).print("warning")
    env.execute("ad count statistics job")
  }
}

class AdCountAgg() extends AggregateFunction[AdClickLog, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: AdClickLog, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class AdCountWindowResult() extends WindowFunction[Long, AdClickCountByProvince, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdClickCountByProvince]): Unit = {
    val end = new Timestamp(window.getEnd).toString
    out.collect(AdClickCountByProvince(end, key, input.head))
  }
}

class FilterBlackListUserResult(maxCount: Long) extends KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog] {
  // 定义状态，保存用户对广告的点击量，每天0点定时清空状态的时间戳，标记当前用户是否已经进入黑名单
  lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count", classOf[Long]))
  lazy val resetTimerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("reset-ts", classOf[Long]))
  lazy val isBlackState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-black", classOf[Boolean]))

  override def processElement(i: AdClickLog, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context, collector: Collector[AdClickLog]): Unit = {
    val curCount = countState.value()

    // 判断只要是第一个数据来了，直接注册0点的清空状态定时器
    if (curCount == 0) {
      val ts = (ctx.timerService().currentProcessingTime()/(1000 * 60 * 60 * 24) + 1) * (24 * 60 * 60 * 1000) - 8 * (24 * 60 * 60 * 1000)
      resetTimerTsState.update(ts)
      ctx.timerService().registerProcessingTimeTimer(ts)
    }


  }
}