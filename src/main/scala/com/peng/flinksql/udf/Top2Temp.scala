package com.peng.flinksql.udf

import com.peng.flinksql.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

object Top2Temp {

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

    val top2Temp: Top2Temp = new Top2Temp()

    tableData.groupBy('id)
      .flatAggregate(top2Temp('temperature) as('temp, 'rank))
      .select('id, 'temp, 'rank)
      .toRetractStream[Row]
    //      .print("table agg data")

    // todo 不知道怎么sql 实现！！！
//    tableEnv.createTemporaryView("sensor",tableData)
//    tableEnv.registerFunction("udfTop2Temp",top2Temp)
//    tableEnv.sqlQuery(
//      """
//        |select
//        | id
//        |,temp
//        |,rk
//        |from sensor
//        |,lateral table (udfTop2Temp(temperature)) as udfTop2Temp(temp,rk)
//      """.stripMargin)
//        .toRetractStream[Row]
//        .print("sql agg data")

    env.execute("table agg function job")
  }

}

class Top2TempAcc {
  var highestTemp: Double = Double.MinValue
  var secondHighestTemp: Double = Double.MinValue
}

class Top2Temp() extends TableAggregateFunction[(Double, Int), Top2TempAcc] {
  override def createAccumulator(): Top2TempAcc = new Top2TempAcc()

  def accumulate(acc: Top2TempAcc, temp: Double): Unit = {
    if (temp > acc.highestTemp) {
      acc.secondHighestTemp = acc.highestTemp
      acc.highestTemp = temp
    } else if (temp > acc.secondHighestTemp) {
      acc.secondHighestTemp = temp
    }
  }

  def emitValue(acc: Top2TempAcc, out: Collector[(Double, Int)]): Unit = {
    out.collect((acc.highestTemp, 1))
    out.collect((acc.secondHighestTemp, 2))
  }
}

