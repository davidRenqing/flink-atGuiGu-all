package com.atguigu.hotitems_analysis

import java.util.Properties

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**gcs:导入这个包，在 addSource 那里才不会报错 */
import org.apache.flink.streaming.api.scala._

case class UserBehavior2( userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long, conter:Long)

object metricTest {

    def main(args: Array[String]): Unit = {
//        val env = StreamExecutionEnvironment.getExecutionEnvironment
//
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//
//        val properties = new Properties()
//        properties.setProperty("bootstrap.servers", "localhost:9092")
//        properties.setProperty("group.id", "consumer-group")
//        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
//        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
//        properties.setProperty("auto.offset.reset", "latest")
//
//        val sourceStream = env.addSource( new FlinkKafkaConsumer[String]("sunday", new SimpleStringSchema(), properties) )
//        sourceStream.map(new RichMapFunction[String] {
//            override def map(value: String): String = {
//                ""
//            }
//        })

        //==========================================================1
        /**gcs:
          * 创建环境
          * */
        // 1. 创建执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        //==========================================================2
        /**gcs:
          * 设定默认的并行度为1
          * */
        env.setParallelism(1)

        //==========================================================3
        /**gcs:
          * 设置 EventTime
          * */
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        //==========================================================4
        /**gcs:
          * 从 kafka 中读取数据
          * */
        // 2. 读取数据
        val properties = new Properties()
        properties.setProperty("bootstrap.servers", "localhost:9092")
        properties.setProperty("group.id", "consumer-group")
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("auto.offset.reset", "latest")

        //==========================================================5
        /**gcs:
          * 从 Kafka 中读取topic
          * */
        //    val sourceStream = env.readTextFile("/Users/congshuo.geng/Documents/david/flink-尚硅谷/3.代码/flink-atGuiGu-all/UserBehaviorAnalysis/HotItemsAnalysis/src/main/resources/UserBehavior.csv")
        val sourceStream = env.addSource( new FlinkKafkaConsumer[String]("hotItems", new SimpleStringSchema(), properties) )

        val dataStream = sourceStream.map(new RichMapFunction[String,UserBehavior2] {

            @transient private var myCounter:Counter = _

            override def open(parameters: Configuration): Unit = {
                myCounter = getRuntimeContext.getMetricGroup.counter("myCounter")
            }

            override def map(value: String):UserBehavior2 ={
                val dataArray = value.split(",")

                myCounter.inc()
                UserBehavior2( dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong,myCounter.getCount )
            }
        })

        dataStream.print("dataStream")

        env.execute("metric test")

    }



}
