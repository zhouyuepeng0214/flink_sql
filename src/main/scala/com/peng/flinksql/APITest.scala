package com.peng.flinksql


import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table, TableEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors._

object APITest {

  def main(args: Array[String]): Unit = {

    //流式环境
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    val oldStreamSettings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()

//    val blinkStreamSettings: EnvironmentSettings = EnvironmentSettings.newInstance()
//      .useBlinkPlanner()
//      .inStreamingMode()
//      .build()

    val oldTableStreamEnv: StreamTableEnvironment = StreamTableEnvironment.create(streamEnv, oldStreamSettings)
//    val blinkTableStreamEnv: StreamTableEnvironment = StreamTableEnvironment.create(streamEnv, blinkStreamSettings)

    //批处理环境
    //    val batchEnv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //    val blinkBatchSettings: EnvironmentSettings = EnvironmentSettings.newInstance()
    //      .useBlinkPlanner()
    //      .inBatchMode()
    //      .build()

    //    val oldTableBatchEnv: BatchTableEnvironment = BatchTableEnvironment.create(batchEnv)
    //    val blinkTableBatchEnv: TableEnvironment = TableEnvironment.create(blinkBatchSettings)

    //流式从文件创建table
    val filePath: String = "E:\\ZYP\\code\\flink_sql\\src\\main\\resources\\sensor.txt"
    oldTableStreamEnv
      .connect(new FileSystem().path(filePath))
      .withFormat(new Csv)
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE()))
      .createTemporaryTable("inputTable")

    val sensorTable: Table = oldTableStreamEnv.from("inputTable")
    sensorTable.toAppendStream[(String, Long, Double)].print("input table result")

    //Table API
    val resultTable: Table = sensorTable
      .filter("id = 'sensor_1'")
      .select("id,temperature")
    resultTable.toAppendStream[(String, Double)].print("table result")

    val aggResultTable: Table = sensorTable.groupBy('id)
      .select('id,'id.count as 'count)
    val aggResult: DataStream[(Boolean, (String,Long))] = aggResultTable.toRetractStream[(String,Long)]
    aggResult.print("agg result")

    //sql 查询
    //oldTableStreamEnv.registerTable("sensorTable",sensorTable)
    oldTableStreamEnv.createTemporaryView("sensorTable", sensorTable)
    val sqlResultTable: Table = oldTableStreamEnv.sqlQuery(
      """
        |select id,temperature
        |from sensorTable
        |where id = 'sensor_1'
      """.stripMargin)
    sqlResultTable.toAppendStream[(String, Double)].print("sql result data")

    oldTableStreamEnv.createTemporaryView("sensorTable1", sensorTable)
    val sqlResultTable1: Table = oldTableStreamEnv.sqlQuery(
      """
        |select id,count(id) as cnt
        |from sensorTable1
        |group by id
      """.stripMargin)
    sqlResultTable1.toRetractStream[(String, Long)].print("sql1111 result data")

    //从kafka创建表
//    oldTableStreamEnv.connect(new Kafka().version("0.11")
//      .topic("sensor")
//      .property("zookeeper.conncect", "hadoop102:2181")
//      .property("bootstrap.servers", "hadoop102:9092"))
//      .withFormat(new Csv())
//      .withSchema(new Schema().field("id", DataTypes.STRING())
//        .field("timestamp", DataTypes.BIGINT())
//        .field("temperature", DataTypes.DOUBLE()))
//      .createTemporaryTable("kafkaInputTable")
//    val kafkaSensorTable: Table = oldTableStreamEnv.from("kafkaInputTable")

    streamEnv.execute()

  }

}
