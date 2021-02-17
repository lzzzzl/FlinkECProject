package com.zzl.hotitems

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Slide}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object HotItemsWithSql {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val inputStream = env.readTextFile("/Users/zhengzonglin/IdeaProjects/FlinkDemo/HotItemsAnalysis/src/main/resources/UserBehavior.csv")
    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 定义表执行环境
    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    // 基于DataStream创建Table
    val dataTable = tableEnv.fromDataStream(dataStream, 'itemId, 'behavior, 'timestamp.rowtime as 'ts)

    // Table API进行开窗聚合统计
    val aggTable = dataTable
      .filter('behavior === "pv")
      // 先开窗
      .window(Slide over 1.hours every 5.minutes on 'ts as 'sw)
      .groupBy('itemId, 'sw)
      .select('itemId, 'sw.end as 'windowEnd, 'itemId.count as 'cnt)

    // 用SQL去实现TopN的选取
    tableEnv.createTemporaryView("aggTable", aggTable, 'itemId, 'windowEnd, 'cnt)
    val resultTable = tableEnv.sqlQuery(
      """
        |SELECT *
        |FROM (
        | SELECT
        |   *,
        |   ROW_NUMBER() OVER (PARTITION BY windowEnd ORDER BY cnt DESC) as row_num
        | FROM aggTable)
        |WHERE row_num <= 5
        |""".stripMargin
    )

    resultTable.toRetractStream[Row].print()

    env.execute("HotItems With Sql")
  }
}
