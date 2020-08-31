package com.atguigu.networkflow_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.networkflow_analysis
  * Version: 1.0
  *
  * Created by wushengran on 2019/9/23 10:28
  */

// 定义输入数据的样例类
case class UserBehavior( userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long )

object PageView {
  def main(args: Array[String]): Unit = {
    //==========================================================1
    /**gcs:
      * 设置环境变量
      * */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //==========================================================2
    /**gcs:
      * 设置使用 eventTime
      * */
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //==========================================================3
    /**gcs:
      * 获得当前路径的相对路径
      * */
    // 用相对路径定义数据源
    /**gcs:
      * file:/Users/congshuo.geng/Documents/david/flink-%e5%b0%9a%e7%a1%85%e8%b0%b7/3.%e4%bb%a3%e7%a0%81/flink-atGuiGu-all/UserBehaviorAnalysis/NetworkFlowAnalysis/target/classes/UserBehavior.csv
      * */
    val resource = getClass.getResource("/UserBehavior.csv")

    println(s"-------------------------------------------------0")
    println(resource.getPath)


//    val sourceStream = env.readTextFile(resource.getPath)
    val sourceStream = env.readTextFile("/Users/congshuo.geng/Documents/david/flink-尚硅谷/3.代码/flink-atGuiGu-all/UserBehaviorAnalysis/NetworkFlowAnalysis/src/main/resources/UserBehavior.csv")
    val dataStream = sourceStream
        //==========================================================4
        /**gcs:
          * 数据的规整化
          * */
      .map( data => {
        val dataArray = data.split(",")
        UserBehavior( dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong )
      } )
        //==========================================================5
        /**gcs:
          * 设定日志中的 timestamp 字段为 eventTime 字段
          * */
      .assignAscendingTimestamps(_.timestamp * 1000L)
        //==========================================================6
        /**gcs:
          * 日志进行filter 的操作
          * */
      .filter( _.behavior == "pv" )    // 只统计pv操作
      .map( data => ("pv", 1) )
      .keyBy(_._1)
        //==========================================================7
        /**gcs:
          * 进行 windowAll 的操作。将所有的数据都放到1组中去统计，每隔1h就会输出一次，输出 PV值
          * */
      .timeWindow(Time.hours(1))
      .sum(1)

    dataStream.print("pv count")

    env.execute("page view jpb")
  }
}
