package com.zzl.hotitems

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp
import scala.collection.mutable.ListBuffer


// 定义输入数据样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)
// 定义窗口聚合结果样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object HotItems {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置并行度
    env.setParallelism(1)
    // 定义事件时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 从文件中读取数据，并转换成样例类，提取时间戳生成watermark
    val inputStream = env.readTextFile("/Users/zhengzonglin/IdeaProjects/FlinkDemo/HotItemsAnalysis/src/main/resources/UserBehavior.csv")
    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 得到窗口聚合结果
    val aggStream = dataStream
      // 过滤pv行为
      .filter(_.behavior == "pv")
      // 按照商品id进行分组
      .keyBy("itemId")
      // 设置滑动窗口进行统计
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new CountAgg(), new ItemViewWindowResult())

    val resultStream = aggStream
      // 按照窗口分组，收集当前窗口内的商品count数据
      .keyBy("windowEnd")
      // 自定义处理流程
      .process(new TopNHotItems(5))

    resultStream.print()

    env.execute("hot items")

  }
}

// 自定义预聚合函数AggregateFunction，聚合状态是商品的count值
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
  override def createAccumulator(): Long = 0L

  // 每来一条数据调用一次add，count值加一
  override def add(in: UserBehavior, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

// 自定义窗口函数WindowFunction
class ItemViewWindowResult() extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId = key.asInstanceOf[Tuple1[Long]].f0
    val windowEnd = window.getEnd
    val count = input.iterator.next()
    out.collect(ItemViewCount(itemId, windowEnd, count))
  }
}

// 自定义KeyProcessFunction
class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {
  // 先定义状态：ListState
  private var itemViewCountListState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    itemViewCountListState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemViewCount-list",
      classOf[ItemViewCount]))
  }

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context,
                              out: Collector[String]): Unit = {
    // 每来一条数据，直接加入ListState
    itemViewCountListState.add(value)
    // 注册一个windowEnd+1之后触发的定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  // 当定时器触发，可以认为所有窗口统计结果都已经到齐，可以排序输出
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 为了方便排序，另外定义一个ListBuffer，保存ListState里面所有的数据
    val allItemViewCounts: ListBuffer[ItemViewCount] = ListBuffer()
    val iter = itemViewCountListState.get().iterator()
    while (iter.hasNext) {
      allItemViewCounts += iter.next()
    }

    // 清空状态
    itemViewCountListState.clear()

    // 按照count大小排序，取前N个
    val sortedItemViewCounts = allItemViewCounts.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    // 将排名信息格式化成String，便于打印输出可视化展示
    val result: StringBuilder = new StringBuilder
    result.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n")

    // 遍历结果列表中的每个ItemViewCount，输出到一行
    for (i <- sortedItemViewCounts.indices) {
      val currentItemViewCount = sortedItemViewCounts(i)
      result.append("NO").append(i + 1).append(": \t")
        .append("商品ID = ").append(currentItemViewCount.itemId).append("\t")
        .append("热门度 = ").append(currentItemViewCount.count).append("\n")
    }

    result.append("====================================\n\n")
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}