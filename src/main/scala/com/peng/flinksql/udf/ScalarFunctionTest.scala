package com.peng.flinksql.udf

import com.peng.flinksql.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

object ScalarFunctionTest {

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
    val sensorTable: Table = tableEnv.fromDataStream(dataStream,'id, 'temperature, 'time, 'time.rowtime as 'tr)
    val udfHashCode: HashCode = new HashCode(1.23)

    //Table API 调用
    val tableData: Table = sensorTable.select('id, 'tr, udfHashCode('id))
    tableData.toAppendStream[Row].print("table data")

    tableEnv.createTemporaryView("sensor",sensorTable)
    // todo sql方式调用udf，首先要先注册函数
    tableEnv.registerFunction("udfHashCode",udfHashCode)
    tableEnv.sqlQuery(
      """
        |select
        | id
        |,tr
        |,udfHashCode(id) as id_hash
        |from sensor
      """.stripMargin).toAppendStream[Row].print("sql data")

    env.execute("scalar udf test")
  }

}

class HashCode(factor: Double) extends ScalarFunction{
  def eval(in: String): Int = {
    (in.hashCode * factor).toInt
  }
}
