package com.peng.flinksql

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Rowtime, Schema}
import org.apache.flink.types.Row

object TimeAndWindowTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
//    val inputStream: DataStream[String] = env.readTextFile("E:\\ZYP\\code\\flink_sql\\src\\main\\resources\\sensor.txt")
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
    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'time, 'time.rowtime as 'tr)
    sensorTable.toAppendStream[Row].print("sensor table")
    //滚动窗口
//    sensorTable
//      .window(Tumble over 10.seconds on 'tr as 'tsw)
//      .groupBy('tsw, 'id)
//      .select('id, 'id.count as 'cnt, 'tsw.start, 'tsw.end, 'tsw.rowtime)
//      .toAppendStream[Row].print("test data")

    //滑动窗口
//    sensorTable.window(Slide over 10.seconds every 5.second on 'tr as 'trw)
//        .groupBy('trw, 'id)
//        .select('id, 'id.count, 'trw.start, 'trw.end, 'trw.rowtime)
//        .toAppendStream[Row].print("test data")
    //会话窗口
//    sensorTable.window(Session withGap 10.seconds on 'tr as 'sets)
//        .groupBy('sets, 'id)
//        .select('id, 'id.count as 'cnt, 'sets.start, 'sets.end, 'sets.rowtime)
//        .toAppendStream[Row].print("test data")

    //over window
//    sensorTable.window(Over partitionBy 'id orderBy 'tr preceding 10.seconds as 'ow)
//        .select('id, 'id.count over 'ow, 'temperature.avg over 'ow, 'time, 'tr)
//        .toAppendStream[Row].print("test data")

//    //sql实现
    tableEnv.createTemporaryView("sensorTable",sensorTable)
    //只能是second等单数形式
//    tableEnv.sqlQuery(
//      """
//        |select
//        | id
//        |,count(id)
//        |,tumble_start(tr,interval '10' second) as start1
//        |,tumble_end(tr,interval '10' second) as end1
//        |from sensorTable
//        |group by id,tumble(tr,interval '10' second)
//      """.stripMargin).toRetractStream[Row].print("test data")

    //over window
//    tableEnv.sqlQuery(
//      """
//        |select
//        | id
//        |,count(id) over w
//        |,avg(temperature) over w
//        |from sensorTable
//        |window w as (
//        |  partition by id
//        |  order by tr
//        |  rows between 2 preceding and current row
//        |)
//      """.stripMargin).toAppendStream[Row].print("test data")

    //over window
    tableEnv.sqlQuery(
      """
        |select
        | id
        |,count(id) over w
        |,avg(temperature) over w
        |from sensorTable
        |window w as (
        | partition by id
        | order by tr
        | range between interval '5' second preceding and current row
        |)
      """.stripMargin).toAppendStream[Row].print("sql window time")


    //tableAPI接收数据
    //        tableEnv.connect(new FileSystem().path("E:\\ZYP\\code\\flink_sql\\src\\main\\resources\\sensor.txt"))
    //          .withFormat(new Csv())
    //          .withSchema(new Schema().field("id", DataTypes.STRING())
    //            .field("timestamp", DataTypes.BIGINT())
    //            .rowtime(new Rowtime().timestampsFromField("timestamp")
    //              .watermarksPeriodicBounded(1000L))
    //            .field("temperature", DataTypes.DOUBLE()))
    //          .createTemporaryTable("inputTable")
    //    val sensorTable: Table = tableEnv.from("inputTable")
    //    sensorTable.select('id, 'timestamp, 'temperature).toAppendStream[Row].print("connect data")

    //sql接收数据
    //        val sinkDDL: String =
    //      """
    //        |create table dataTable(
    //        |id string not null,
    //        |ts bigint,
    //        |temperature double
    //        |) with (
    //        |'connector.type' = 'filesystem',
    //        |'connector.path' = 'file:///E:\\ZYP\\code\\flink_sql\\src\\main\\resources\\sensor.txt',
    //        |'format.type' = 'csv'
    //        |)
    //      """.stripMargin
    //    tableEnv.sqlUpdate(sinkDDL)

    //    val table: Table = tableEnv.sqlQuery(
    //      """
    //        |select
    //        | id
    //        |,ts
    //        |,temperature
    //        |from dataTable
    //        |where id = 'sensor_1'
    //      """.stripMargin)
    //    table.toAppendStream[Row].print("sql data")

    env.execute("time and window test job")
  }

}
