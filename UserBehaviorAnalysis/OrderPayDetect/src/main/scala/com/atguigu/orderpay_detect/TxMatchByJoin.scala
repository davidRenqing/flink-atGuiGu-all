package com.atguigu.orderpay_detect

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.orderpay_detect
  * Version: 1.0
  *
  * Created by wushengran on 2019/9/25 15:40
  */
object TxMatchByJoin {
  def main(args: Array[String]): Unit = {
    //==========================================================1
    /**gcs:
      * 创建变量
      * */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //==========================================================2
    /**gcs:
      * 设置日志类型为eventTime 类型
      * */
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //==========================================================3
    /**gcs:
      * 读取源数据
      * */
    // 读取订单事件流
    val resource = getClass.getResource("/OrderLog.csv")
    //    val orderEventStream = env.readTextFile(resource.getPath)
    val orderEventStream = env.socketTextStream("localhost", 7777)
      .map(data => {
        //==========================================================4
        /**gcs:
          * 进行数据的整理
          * */
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
        //==========================================================5
        /**gcs:
          * 将 txId 为空字符串的信息给删除掉
          * */
      .filter(_.txId != "")
        //==========================================================6
        /**gcs:
          * 设定 waterMark
          * */
      .assignAscendingTimestamps(_.eventTime * 1000L)
        //==========================================================7
        /**gcs:
          * keyBy txId
          * */
      .keyBy(_.txId)

    //==========================================================8
    /**gcs:
      * 读取 到账事件流
      * */
    // 读取支付到账事件流
    val receiptResource = getClass.getResource("/ReceiptLog.csv")
    //    val receiptEventStream = env.readTextFile(receiptResource.getPath)
    val receiptEventStream = env.socketTextStream("localhost", 8888)
      .map( data => {
        val dataArray = data.split(",")
        ReceiptEvent( dataArray(0).trim, dataArray(1).trim, dataArray(2).toLong )
      } )
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    //==========================================================9
    /**gcs:
      * 将两个流 join 在一起
      * */
    // join处理
    val processedStream = orderEventStream.intervalJoin( receiptEventStream )
        //==========================================================10
        /**gcs:
          * 设定
          * */
      .between(Time.seconds(-5), Time.seconds(5))
      .process( new TxPayMatchByJoin() )

    processedStream.print()

    env.execute("tx pay match by join job")
  }
}

//==========================================================11
/**gcs:
  * 实现 processJoinFunction 类
  * */
class TxPayMatchByJoin() extends ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]{
  //==========================================================f1
  /**gcs:
    * 执行 processElement 每条数据
    * */
  override def processElement(left: OrderEvent, right: ReceiptEvent, ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    out.collect((left, right))
  }
}
