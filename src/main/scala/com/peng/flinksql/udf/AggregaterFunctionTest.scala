package com.peng.flinksql.udf

import com.peng.flinksql.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.types.Row

object AggregaterFunctionTest {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //    val inputStream: DataStream[String] = env.socketTextStream("localhost",7777)
    val inputStream: DataStream[String] = env.readTextFile("E:\\ZYP\\code\\flink_sql\\src\\main\\resources\\sensor.txt")
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

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    val tableData: Table = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'time, 'time.rowtime as 'tr)

    //table API 实现
    val udfAvgTemp: AvgTemp = new AvgTemp()
    tableData.groupBy('id)
      .aggregate(udfAvgTemp('temperature) as 'avgTemp)
      .select('id, 'avgTemp)
      .toRetractStream[Row]
    //      .print("table data")

    //sql 实现
    tableEnv.createTemporaryView("sensor", tableData)
    tableEnv.registerFunction("udfAvgTemp", udfAvgTemp)
    tableEnv.sqlQuery(
      """
        |select
        | id
        |,udfAvgTemp(temperature) as avg_temp
        |from sensor
        |group by id
      """.stripMargin)
      .toRetractStream[Row]
      .filter(_._1)
      .map(_._2)
      .print("sql data")

    env.execute("scalar udf test")
  }
}

//自定义一个聚合函数 同avg
class AvgTemp extends AggregateFunction[Double, AvgTempAcc] {
  override def getValue(accumulator: AvgTempAcc): Double = accumulator.sum / accumulator.count

  override def createAccumulator(): AvgTempAcc = new AvgTempAcc()

  def accumulate(acc: AvgTempAcc, temp: Double): Unit = {
    acc.sum += temp
    acc.count += 1
  }
}

//定义一个聚合函数的状态类，用于保存聚合状态（sum,count）
class AvgTempAcc {
  var sum: Double = 0.0
  var count: Int = 0
}