package com.zzl.orderpaydetect

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import java.util

// 定义输入输出样例类类型
case class OrderEvent(orderId: Long, eventType: String, txId: String, timestamp: Long)
case class OrderResult(orderId: Long, resultMsg: String)

// CEP实现方式
object OrderTimeOut {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream = env.readTextFile("/Users/zhengzonglin/IdeaProjects/FlinkDemo/OrderPayDetect/src/main/resources/OrderLog.csv")
    val orderEventStream = inputStream
      .map(data => {
        val arr = data.split(",")
        OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .keyBy(_.orderId)

    // 1. 定义一个pattern
    val orderPayPattern = Pattern
      .begin[OrderEvent]("create").where(_.eventType == "create")
      .followedBy("pay").where(_.eventType == "pay")
      .within(Time.minutes(15))

    // 2. 将pattern应用到数据流上，进行模式检测
    val patternStream = CEP.pattern(orderEventStream, orderPayPattern)

    // 3. 定义侧输出流标签
    val orderTimeoutOutputTag = new OutputTag[OrderResult]("orderTimeOut")

    // 4. 调用select方法，提取并处理匹配的成功支付事件以及超时事件
    val resultStream = patternStream.select(
      orderTimeoutOutputTag,
      new OrderTimeoutSelect(),
      new OrderPaySelect()
    )

    resultStream.print()
    resultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")

    env.execute("order timeout job")
  }
}

// 实现自定义的PatternTimeoutFunction以及PatternSelectFunction
class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult] {
  override def timeout(pattern: util.Map[String, util.List[OrderEvent]], timeoutTimestamp: Long): OrderResult = {
    val timeoutOrderId = pattern.get("create").iterator().next().orderId
    OrderResult(timeoutOrderId, "timeout " + ": +" + timeoutTimestamp)
  }
}

class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderResult] {
  override def select(pattern: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val payedOrderId = pattern.get("pay").iterator().next().orderId
    OrderResult(payedOrderId, "payed successfully")
  }
}
