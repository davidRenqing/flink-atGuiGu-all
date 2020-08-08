package com.atguigu.apitest.sinktest

import java.util.Properties

import com.atguigu.apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011.Semantic
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

object KafkaSinkTest {
  def main(args: Array[String]): Unit = {
    //==========================================================1
    /**gcs:
      * 创建环境
      * */
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //==========================================================2
    /**gcs:
      * 设置并行度
      * */
    env.setParallelism(1)

    //==========================================================3
    /**gcs:
      * 设置从 kafka中读取数据的参数
      * */
    // source
//    val inputStream = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt")
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    //==========================================================4
    /**gcs:
      * 从 kafka 中读取数据
      * */
    val inputStream = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))

    // Transform操作


    val dataStream = inputStream
      .map(
        data => {
          val dataArray = data.split(",")
          SensorReading( dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble ).toString  // 转成String方便序列化输出
        }
      )

    //==========================================================5
    /**gcs:
      * 将数据落地到 kafka 的 Producer 中
      * */
    // sink
    dataStream.addSink( new FlinkKafkaProducer011[String]( "sinkTest", new SimpleStringSchema(), properties) )
    dataStream.print()

    env.execute("kafka sink test")
  }
}
