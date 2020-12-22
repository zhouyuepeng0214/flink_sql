package com.peng.flinksql

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._

object Example {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputPath: String = "E:\\ZYP\\code\\flink_sql\\src\\main\\resources\\sensor.txt"
    val inputStream: DataStream[String] = env.readTextFile(inputPath)
//    val inputStream: DataStream[String] = env.socketTextStream("localhost",7777)
    val mapStream: DataStream[SensorReading] = inputStream.map(data => {
      val dataArray: Array[String] = data.split(",")
      SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
    })
    mapStream.print("input data")

    val streamTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    val dataTable: Table = streamTableEnv.fromDataStream(mapStream)

    //执行计划
    val stringExplain: String = streamTableEnv.explain(dataTable)
    println(stringExplain)

    val resultTable: Table = dataTable.select("id,temperature")
      .filter("id = 'sensor_1'") // == 与 = 同效果
    resultTable.toAppendStream[(String,Double)].print("table result")

//    streamTableEnv.registerTable("dataTable",dataTable)
    streamTableEnv.createTemporaryView("dataTable",dataTable)
    val resultSqlTable: Table = streamTableEnv.sqlQuery("select id,temperature from dataTable where id = 'sensor_1'".stripMargin)

    resultSqlTable.toAppendStream[(String,Double)].print("sql result")

    env.execute("flink sql test")
  }

}

case class SensorReading(id: String, time: Long, temperature: Double) {}
