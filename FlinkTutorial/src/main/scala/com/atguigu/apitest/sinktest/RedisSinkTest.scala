package com.atguigu.apitest.sinktest

import com.atguigu.apitest.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest.sinktest
  * Version: 1.0
  *
  * Created by wushengran on 2019/9/17 16:12
  */
object RedisSinkTest {
  def main(args: Array[String]): Unit = {
    //==========================================================1
    /**gcs:
      * 创建环境
      * */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //==========================================================2
    /**gcs:
      * 从 txt 中读取数据。形成 inputStream
      * */
    // source
    val inputStream = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    //==========================================================3
    /**gcs:
      * 之后将数据进行转换，封装成 SensorReading 的数据
      * */
    // transform
    val dataStream = inputStream
      .map(
        data => {
          val dataArray = data.split(",")
          SensorReading( dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble )
        }
      )

    //==========================================================4
    /**gcs:
      * 创建一个Jedis的配置连接
      * */
    val conf = new FlinkJedisPoolConfig.Builder()
      .setHost("localhost")
      .setPort(6379)
      .build()

    //==========================================================5
    /**gcs:
      *创建一个 redis 的 Sink 流
      **/
    // sink
    dataStream.addSink( new RedisSink(conf, new MyRedisMapper()) )

    env.execute("redis sink test")
  }
}

class MyRedisMapper() extends RedisMapper[SensorReading]{

  // 定义保存数据到redis的命令
  override def getCommandDescription: RedisCommandDescription = {
    // 把传感器id和温度值保存成哈希表 HSET key field value
    new RedisCommandDescription( RedisCommand.HSET, "sensor_temperature" )
  }

  // 定义保存到redis的value
  override def getValueFromData(t: SensorReading): String = t.temperature.toString

  // 定义保存到redis的key
  override def getKeyFromData(t: SensorReading): String = t.id
}