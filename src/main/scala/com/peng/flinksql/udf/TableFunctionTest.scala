package com.peng.flinksql.udf

import com.peng.flinksql.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

object TableFunctionTest {


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

    val udfSplit: UdfSplit = new UdfSplit("_")

    //侧向连接，应用TableFunction
//    tableData.joinLateral(udfSplit('id) as ('word, 'length))
//        .select('id, 'tr, 'word, 'length)
//        .toAppendStream[Row].print("table data")
//
//    tableData.leftOuterJoinLateral(udfSplit('id) as ('word,'length))
//        .select('id, 'tr, 'word, 'length)
//        .toAppendStream[Row].print("table data2")

    //sql实现
    tableEnv.createTemporaryView("sensor",tableData)
    tableEnv.registerFunction("udfSplit",udfSplit)
    // todo sql实现
    tableEnv.sqlQuery(
      """
        |select
        | id
        |,tr
        |,word
        |,length
        |from sensor
        |,lateral table (udfSplit(id)) as splitid(word,length)
      """.stripMargin)
        .toAppendStream[Row].print("sql data1")

    tableEnv.sqlQuery(
      """
        |select
        | id
        |,tr
        |,word
        |,length
        |from sensor
        |left join
        |  lateral table(udfSplit(id)) as newSensor(word,length)
        |  on true
      """.stripMargin)
        .toAppendStream[Row].print("sql data2")

    env.execute("scalar udf test")
  }
}

//自定义tableFunction,实现分割字符串并统计长度（word,length）
class UdfSplit(separator: String) extends TableFunction[(String, Int)] {
  def eval(str: String): Unit = {
    val dataArray: Array[String] = str.split(separator)
    dataArray.foreach(
      word => collect((word, word.length()))
    )
  }

}