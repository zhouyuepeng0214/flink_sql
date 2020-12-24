package com.peng.flinksql

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, Table, Tumble}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object SensorExer {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream: DataStream[String] = env.socketTextStream("localhost",7777)
    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val dataArray: Array[String] = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(t: SensorReading): Long = {
          t.time * 1000L
        }
      })
    dataStream.print("input data")

    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env,settings)
    val dataTable: Table = tableEnv.fromDataStream(dataStream,'id, 'temperature, 'time, 'time.rowtime as 'tr)
    val resultTable: Table = dataTable.window(Tumble over 10.seconds on 'tr as 'w)
      .groupBy('w, 'id)
      .select('id, 'id.count as 'cnt)
    resultTable.toAppendStream[Row].print("table data test")

    tableEnv.createTemporaryView("sqlTable",dataTable)
    tableEnv.sqlQuery(
      """
        |select
        | id
        |,count(id) as cnt
        |from sqlTable
        |group by id,tumble(tr,interval '10' second)
      """.stripMargin).toAppendStream[Row].print("sql data test")

    env.execute("xuqiu exer job")
  }

}
