package com.atguigu.hotitems_analysis

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaProducer {
  def main(args: Array[String]): Unit = {
    //==========================================================1
    writeToKafka("hotItems",10)
  }
  //==========================================================1.1
  /**gcs:
    * kafka 产出数据的含糊
    * */
  def writeToKafka(topic: String,timeMillis:Long=0): Unit ={
    //==========================================================f1
    /**gcs:
      * 配置
      * */
    val properties = new Properties()
    properties.put("bootstrap.servers", "localhost:9092") //zookeeper 的地址
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer") //key的序列化方式
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer") //value 的序列化方式

    //==========================================================f2
    /**gcs:
      * 创建 producer
      * */
    // 定义一个kafka producer
    val producer = new KafkaProducer[String, String](properties)
    //==========================================================f3
    /**gcs:
      * 从csv 中读取文件
      * */
    // 从文件中读取数据，发送
    val bufferedSource = io.Source.fromFile( "/Users/congshuo.geng/Documents/david/flink-尚硅谷/3.代码/flink-atGuiGu-all/UserBehaviorAnalysis/HotItemsAnalysis/src/main/resources/UserBehavior.csv" )

    //==========================================================f4
    /**gcs:
      * 进行for 循环，将从文件流中读到的每一条数据进行遍历，输入到kafka的producer中
      * */
    for( line <- bufferedSource.getLines() ){
      val record = new ProducerRecord[String, String](topic, line)
      //==========================================================f5
      /**gcs:
        * 用 producer 像 kafak 的 topic 为 sunday 的位置发送数据
        * */
      producer.send(record)
      //==========================================================f6
      /**gcs:
        * 没发一条记录，就睡 millis
        * */
      println(s"-------------------------------------------------0")
      Thread.sleep(timeMillis)
    }
    producer.close()
  }
}