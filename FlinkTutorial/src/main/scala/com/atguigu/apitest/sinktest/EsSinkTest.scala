package com.atguigu.apitest.sinktest

import java.util

import com.atguigu.apitest.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest.sinktest
  * Version: 1.0
  *
  * Created by wushengran on 2019/9/17 16:27
  */
object EsSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // source
    val inputStream = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    // transform
    val dataStream = inputStream
      .map(
        data => {
          val dataArray = data.split(",")
          SensorReading( dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble )
        }
      )

    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("localhost", 9200))

    // 创建一个esSink 的builder
    val esSinkBuilder = new ElasticsearchSink.Builder[SensorReading](
      httpHosts,
      new ElasticsearchSinkFunction[SensorReading] {
        override def process(element: SensorReading, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
          println("saving data: " + element)
          // 包装成一个Map或者JsonObject
          val json = new util.HashMap[String, String]()
          json.put("sensor_id", element.id)
          json.put("temperature", element.temperature.toString)
          json.put("ts", element.timestamp.toString)

          // 创建index request，准备发送数据
          val indexRequest = Requests.indexRequest()
            .index("sensor")
            .`type`("readingdata")
            .source(json)

          // 利用index发送请求，写入数据
          indexer.add(indexRequest)
          println("data saved.")
        }
      }
    )

    // sink
    dataStream.addSink( esSinkBuilder.build() )

    env.execute("es sink test")
  }
}
