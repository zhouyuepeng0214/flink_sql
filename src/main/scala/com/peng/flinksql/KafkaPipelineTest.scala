package com.peng.flinksql

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}

object KafkaPipelineTest {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    tableEnv
      .connect(new Kafka()
        .version("0.11")
        .topic("sensor")
        .property("zookeeper.connect", "hadoop102:2181")
        .property("bootstrap.servers", "hadoop102:9092"))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
        .field("pt", DataTypes.TIMESTAMP(3))
        .proctime())
      .createTemporaryTable("kafkaInputTable")
    val sensorTable: Table = tableEnv.from("kafkaInputTable")
    val filterTable: Table = sensorTable
      .filter('id === "sensor_1")
      .select('id, 'temperature)
    filterTable.toAppendStream[(String, Double)].print("filter table")

    val aggTable: Table = sensorTable
      .groupBy('id)
      .select('id, 'id.count as 'count)
    aggTable.toRetractStream[(String, Long)].print("agg table")

    tableEnv
      .connect(new Kafka()
        .version("0.11")
        .topic("sinkTest")
        .property("zookeeper.connect", "hadoop102:2181")
        .property("bootstrap.servers", "hadoop102:9200"))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("temp", DataTypes.DOUBLE())
//        .field("cnt", DataTypes.BIGINT())
      )
      .createTemporaryTable("kafkaOutputTable")

    filterTable.insertInto("kafkaOutputTable")

//    aggTable.insertInto("kafkaOutputTable")

    env.execute("kafka pipeline test")
  }

}
