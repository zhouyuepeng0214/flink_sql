package com.peng.flinksql

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

object FsOutputTest {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    val filePath: String = "E:\\ZYP\\code\\flink_sql\\src\\main\\resources\\sensor.txt"
    val inputStream: DataStream[String] = env.readTextFile(filePath)
    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val dataArray: Array[String] = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })

    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'temperature as 'temp, 'time as 'ts)
    val filterTable: Table = sensorTable.filter('id === "sensor_1") //filter("id = 'sensor_1'")
      .select('id, 'temp)
    filterTable.toAppendStream[(String, Double)].print("filter table")

    val aggResultTable: Table = sensorTable.groupBy('id)
      .select('id, 'id.count as 'count)
    aggResultTable.toRetractStream[(String, Long)].print("agg table")

    val outputFilePath: String = "E:\\ZYP\\code\\flink_sql\\src\\main\\resources\\output.txt"
    tableEnv.connect(new FileSystem().path(outputFilePath))
      .withFormat(new Csv())
      .withSchema(new Schema().field("id", DataTypes.STRING())
        .field("temp", DataTypes.DOUBLE())
        //      .field("cnt",DataTypes.BIGINT())
      )
      .createTemporaryTable("outputTable")

    filterTable.insertInto("outputTable")

    env.execute("fs output test job")
  }

}
