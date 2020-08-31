package com.atguigu.loginfail_detect

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.loginfail_detect
  * Version: 1.0
  *
  * Created by wushengran on 2019/9/24 15:46
  */
object LoginFailWithCep {
  def main(args: Array[String]): Unit = {
    //==========================================================1
    /**gcs:
      * 创建环境变量
      * */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //==========================================================2
    /**gcs:
      * 设定使用 EventTime 的消费的方式
      * */
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //==========================================================3
    /**gcs:
      * 设定并行度为1
      * */
    env.setParallelism(1)

    //==========================================================4
    /**gcs:
      * 读取源数据
      * */
    // 1. 读取事件数据，创建简单事件流
//    val resource = getClass.getResource("/LoginLog.csv")
//    val sourceStream = env.readTextFile(resource.getPath)
    val sourceStream = env.readTextFile("/Users/congshuo.geng/Documents/david/flink-尚硅谷/3.代码/flink-atGuiGu-all/UserBehaviorAnalysis/LoginFailDetect/src/main/resources/LoginLog.csv")
    val loginEventStream = sourceStream
        //==========================================================5
        /**gcs:
          * 数据规整化整理
          * */
      .map( data => {
        val dataArray = data.split(",")
        LoginEvent( dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong )
      } )
        //==========================================================6
        /**gcs:
          * 设定时间戳和waterMark 为 5s
          * */
      .assignTimestampsAndWatermarks( new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
      } )
      .keyBy(_.userId)

    //==========================================================7
    /**gcs:
      * 定义一个复杂模式，要求两次记录的 eventTime 都为 fail，并且这两次 eventTime == fail 的记录得是紧邻，而且必须在 3s 内检索完成这个复杂模式
      * */
    // 2. 定义匹配模式
    val loginFailPattern = Pattern.begin[LoginEvent]("begin").where(_.eventType == "fail")
      .next("next").where(_.eventType == "fail")
        /**gcs:
          * 这种 next方法就可以解决 watermark 中日志迟到的情况。但是这要求我们可以设置合理的 waterMark，即需要让 watermark 至少要大于 3s。这样才可以保证迟到的日志不会被丢掉
          * */
      .within(Time.seconds(3))

    //==========================================================8
    /**gcs:
      * 使用这个复杂模式去我的 source Stream 中进行筛选，把符合模式的记录组合给找出来
      * */
    // 3. 在事件流上应用模式，得到一个pattern stream
    val patternStream = CEP.pattern(loginEventStream, loginFailPattern)

    //==========================================================9
    /**gcs:
      * 使用 select 函数，把符合的序列给找出来
      * */
    // 4. 从pattern stream上应用select function，检出匹配事件序列
    val loginFailDataStream = patternStream.select( new LoginFailMatch() )

    loginFailDataStream.print()

    env.execute("login fail with cep job")
  }
}

//==========================================================9.1
/**gcs:
  * 遍历每一个符合模式的记录组合
  * */
class LoginFailMatch() extends PatternSelectFunction[LoginEvent, Warning]{
  override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
    // 从map中按照名称取出对应的事件
//    val iter = map.get("begin").iterator()
    //==========================================================f1
    /**gcs:
      * 这个 map 存储的是符合我的模式的记录组合
      * */
      /**gcs:
        * 将记录组合中为begin 的记录提取出来
        * */
    val firstFail = map.get("begin").iterator().next()
    //==========================================================f2
    /**gcs:
      * 将记录组合中为 next 的 日志记录取出来
      * */
    val lastFail = map.get("next").iterator().next()

    //==========================================================f3
    /**gcs:
      * 拼接成一个 Warning 日志
      * */
    Warning( firstFail.userId, firstFail.eventTime, lastFail.eventTime, "login fail!" )
  }
}