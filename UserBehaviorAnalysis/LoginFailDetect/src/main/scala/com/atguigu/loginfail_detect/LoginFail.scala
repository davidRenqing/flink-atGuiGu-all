package com.atguigu.loginfail_detect

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.loginfail_detect
  * Version: 1.0/
  *
  * Created by wushengran on 2019/9/24 11:32
  */

// 输入的登录事件样例类
case class LoginEvent( userId: Long, ip: String, eventType: String, eventTime: Long )
// 输出的异常报警信息样例类
case class Warning( userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)

object LoginFail {
  def main(args: Array[String]): Unit = {
    //==========================================================1
    /**gcs:
      * 创建环境变量
      * */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //==========================================================2
    /**gcs:
      * 设置并行度为1
      * */
    env.setParallelism(1)

    //==========================================================3
    /**gcs:
      * 读取源数据
      * */


    // 读取事件数据
    val resource = getClass.getResource("/LoginLog.csv")
//    val sourceStream = env.readTextFile(resource.getPath)

    val sourceStream = env.readTextFile("/Users/congshuo.geng/Documents/david/flink-尚硅谷/3.代码/flink-atGuiGu-all/UserBehaviorAnalysis/LoginFailDetect/src/main/resources/LoginLog.csv")

    val loginEventStream = sourceStream
        //==========================================================4
        /**gcs:
          * 数据整理
          * */
      .map( data => {
        val dataArray = data.split(",")
        LoginEvent( dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong )
      } )
        //==========================================================5
        /**gcs:
          * 设定 waterMark。以及过期时间
          * */
      .assignTimestampsAndWatermarks( new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
      } )

    //==========================================================6
    /**gcs:
      * 按照 userId 进行 keyBy 的操作
      * */
    val warningStream = loginEventStream
      .keyBy(_.userId)    // 以用户id做分组
        //==========================================================7
        /**gcs:
          * 判断一个用户是否在 2s 内连续登陆失败了 maxFailTimes
          * */
      .process( new LoginWarning(2) )

    warningStream.print()
    env.execute("login fail detect job")
  }
}

//==========================================================7.1
/**gcs:
  * 判断同一用户在2s 内连续登陆超过了 maxFailTimes
  * */
class LoginWarning(maxFailTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning]{
  //==========================================================f1
  /**gcs:
    *这个 listState 是存储用户登陆失败的所有的 timestamp 时间戳
    * */
  // 定义状态，保存2秒内的所有登录失败事件
  lazy val loginFailState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login-fail-state", classOf[LoginEvent]))

  //==========================================================f2
  /**gcs:
    * 每来一条数据就会执行这个函数
    * */
  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {

//    //==========================================================f2.1
//    //==========================================================f2.1.1
//    /**gcs:
//      * 获得用户登陆失败的列表
//      * */
//    val loginFailList = loginFailState.get()
//    //==========================================================f2.2.2
//    /**gcs:
//      * 判断用户该用户登陆失败的 eventType 是否是 fail
//      * */
//        // 判断类型是否是fail，只添加fail的事件到状态
//        if( value.eventType == "fail" ){
//          //==========================================================f2.2.3
//          /**gcs:
//            * 如果发现 failList 有失败的数量
//            * */
//          if( ! loginFailList.iterator().hasNext ){
//            //==========================================================f2.2.4
//            /**gcs:
//              * 这时候就设置一个定时器。这个定时器是2s。当2s之后就会触发 onTimer() 函数。
//              * 2s 之后就会判断 loginFailState 中的数目，如果发现大于2个，这时候触发报警
//              * */
//            ctx.timerService().registerEventTimeTimer( value.eventTime * 1000L + 2000L )
//          }
//          loginFailState.add( value )
//        } else {
//          // 如果是成功，清空状态
//          loginFailState.clear()
//        }


    //==========================================================f3
    /**gcs:
      * 如果发现此次用户的登陆仍然显示为失败
      * */
    if( value.eventType == "fail" ){
      //==========================================================f4
      /**gcs:
        * 获得已经存储在 loginFailState 中的上一次用户登陆失败的时间
        * */
      // 如果是失败，判断之前是否有登录失败事件
      val iter = loginFailState.get().iterator()
      if( iter.hasNext ){
        // 如果已经有登录失败事件，就比较事件时间
        val firstFail = iter.next()
        //==========================================================f5
        /**gcs:
          * 如果发现失败的时间间隔小于2s。
          * */
        if( value.eventTime < firstFail.eventTime + 2 ){
          //==========================================================f6
          /**gcs:
            * 发从报警信息
            * */
          // 如果两次间隔小于2秒，输出报警
          out.collect( Warning( value.userId, firstFail.eventTime, value.eventTime, "login fail in 2 seconds." ) )
        }
        //==========================================================f7
        /**gcs:
          * 清空 loginFailState
          * */
        // 更新最近一次的登录失败事件，保存在状态里
        loginFailState.clear()
        //==========================================================f8
        /**gcs:
          * 把最新的用户登陆失败的时间加入到 value 当中
          * */
        loginFailState.add(value)
      } else {

        //==========================================================f9
        /**gcs:
          * 如果该用户是第一次登陆失败。这时候就会把该用户加入到 loginFailState 当中
          * */
        // 如果是第一次登录失败，直接添加到状态
        loginFailState.add(value)
      }
    } else {

      //==========================================================f10
      /**gcs:
        * 如果发现用户上一次登陆成功了，这时候就会清空 state
        * */
      // 如果是成功，清空状态
      loginFailState.clear()
    }
  }

//  //==========================================================f3
//  /**gcs:
//    * onTimer 函数
//    * */
//  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
//    //==========================================================f1
//    /**gcs:
//      * 获取 loginFailList
//      * */
//    // 触发定时器的时候，根据状态里的失败个数决定是否输出报警
//    val allLoginFails: ListBuffer[LoginEvent] = new ListBuffer[LoginEvent]()
//    val iter = loginFailState.get().iterator()
//    //==========================================================f2
//    /**gcs:
//      * 统计出 listState 中的总数目
//      * */
//    while(iter.hasNext){
//      allLoginFails += iter.next()
//    }
//
//    //==========================================================f3
//    /**gcs:
//      *判断listState 中失败的次数，如果失败的次数大于 maxFailTimes，
//      * 这时候就会发送一条报警记录
//      * */
//    // 判断个数
//    if( allLoginFails.length >= maxFailTimes ){
//      out.collect( Warning( allLoginFails.head.userId, allLoginFails.head.eventTime, allLoginFails.last.eventTime, "login fail in 2 seconds for " + allLoginFails.length + " times." ) )
//    }
//    //==========================================================f4
//    /**gcs:
//      * 清空 loginFailState 中的 listState
//      * */
//    // 清空状态
//    loginFailState.clear()
//  }
}
