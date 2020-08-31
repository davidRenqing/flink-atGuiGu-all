package com.atguigu.networkflow_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.networkflow_analysis
  * Version: 1.0
  *
  * Created by wushengran on 2019/9/23 10:43
  */
case class UvCount( windowEnd: Long, uvCount: Long )

object UniqueVisitor {
  def main(args: Array[String]): Unit = {
    //==========================================================1
    /**gcs:
      * 设置环境变量
      * */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)


    // 用相对路径定义数据源
    val resource = getClass.getResource("/UserBehavior.csv")
    val sourceStream = env.readTextFile("/Users/congshuo.geng/Documents/david/flink-尚硅谷/3.代码/flink-atGuiGu-all/UserBehaviorAnalysis/NetworkFlowAnalysis/src/main/resources/UserBehavior.csv")
    val dataStream = sourceStream
        //==========================================================2
        /**gcs:
          * 数据清洗
          * */
      .map( data => {
        val dataArray = data.split(",")
        UserBehavior( dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong )
      } )
        //==========================================================3
        /**gcs:
          * 指定 eventTime 的类型
          * */
      .assignAscendingTimestamps(_.timestamp * 1000L)
        //==========================================================4
        /**gcs:
          * 做 filter 的操作
          * */
      .filter( _.behavior == "pv" )    // 只统计pv操作
        //==========================================================5
        /**gcs:
          * 设定所有的 timeWindow
          * */
      .timeWindowAll( Time.hours(1) )
        //==========================================================6
        /**gcs:
          * 这里的操作，要对当前window中的用户的ID进行filter的操作
          * */
      .apply( new UvCountByWindow() )

    dataStream.print()
    env.execute("uv job")
  }
}

//==========================================================6.1
class UvCountByWindow() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow]{
  //==========================================================f1
  /**gcs:
    * 创建一个 Set，将window窗口中的所有的 userId 都放到 set中，利用set的机制，来自动对该窗口中的数据进行去重的操作
    * */
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    // 定义一个scala set，用于保存所有的数据userId并去重
    var idSet = Set[Long]()
    //==========================================================f2
    /**gcs:
      * 读取当前窗口中的所有的元素，将所有的 userId 的信息放到 set 中，利用 set 的机制对数据进行过滤
      * */
    // 把当前窗口所有数据的ID收集到set中，最后输出set的大小
    for( userBehavior <- input ){
      idSet += userBehavior.userId
    }
    //==========================================================f3
    /**gcs:
      * 利用 set 对 userId 进行过滤之后，将过滤后的 set.size 进行输出
      * */
    out.collect( UvCount( window.getEnd, idSet.size ) )
  }
}
