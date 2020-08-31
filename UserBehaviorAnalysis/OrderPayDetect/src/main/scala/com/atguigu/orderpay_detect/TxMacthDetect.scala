package com.atguigu.orderpay_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.orderpay_detect
  * Version: 1.0
  *
  * Created by wushengran on 2019/9/25 14:15
  */

//==========================================================1
/**gcs:
  *设定接收日志的样例类
  * 交易ID，支付渠道，交易的时间
  * */
// 定义接收流事件的样例类
case class ReceiptEvent(txId: String, payChannel: String, eventTime: Long)

object TxMacthDetect {
    //==========================================================9
    /**gcs:
      * 定义侧输出来侧输出流
      * */
  // 定义侧数据流tag

  /**gcs:
    * 在订单支付的流中找到了订单，但是没有在到账的流拿到数据
    * */
  val unmatchedPays = new OutputTag[OrderEvent]("unmatchedPays")

    /**gcs:
      * 这个侧输出流存储的是，只拿到了到账的流的数据，但是还没有在支付的流中拿到数据
      * */
  val unmatchedReceipts = new OutputTag[ReceiptEvent]("unmatchedReceipts")

  def main(args: Array[String]): Unit = {
      //==========================================================2
      /**gcs:
        * 创建flink的环境变量
        * */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

      //==========================================================3
      /**gcs:
        * 读取数据
        * */
    // 读取订单事件流
    val resource = getClass.getResource("/OrderLog.csv")
//    val orderEventStream = env.readTextFile(resource.getPath)
    val orderEventStream = env.socketTextStream("localhost", 7777)
      .map(data => {
          //==========================================================4
          /**gcs:
            * 使用 map 来对数据进行封装。封装成日志的时间
            * */
      val dataArray = data.split(",")
      OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
    })
      .filter(_.txId != "")
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

      //==========================================================5
      /**gcs:
        * 读取支付到账的流
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
        //==========================================================6
        /**gcs:
          * 将这两个流同时按照 订单ID 来记性 keyBy 操作
          * */
      .keyBy(_.txId)

      //==========================================================7
      /**gcs:
        * 将这两个流使用 connect() 方法连接在一起
        * */
    // 将两条流连接起来，共同处理
    val processedStream = orderEventStream.connect(receiptEventStream)
        //==========================================================8
        /**gcs:
          * 之后使用 coProcessFunction() 来处理这两个流
          * */
      .process( new TxPayMatch() )

    processedStream.print("matched")
    processedStream.getSideOutput(unmatchedPays).print("unmatchedPays")
    processedStream.getSideOutput(unmatchedReceipts).print("unmatchReceipts")

    env.execute("tx match job")
  }

    //==========================================================8.1
    /**gcs:
      * 进行 CoProcessFunction。
      * 这里的 CoProcessFuncrion 还需要指定输入输出的类型。如果 A.connect(B) 。
      * 这时候输入的类型写成 [第一个输入类型，第二个输入类型，输出类型]，因此就写成 [A,B,(A,B)] 4:17
      * */
  class TxPayMatch() extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]{
        //==========================================================f3
        /**gcs:
          * 我们要定义一些状态，让这两个流可以共享这些状态。之后这两个流通过 state 状态来进行信息的交互。
          * 这两个流都是按照 keyBy() 进行操作的。所以，我们可以看到每一个key都会有一个 payState，或者 receiptState 状态
          * */
    // 定义状态来保存已经到达的订单支付事件和到账事件
    /**gcs:
      * 创建 payState。这个state是用来标志用户是否已经做了支付
      * */
    lazy val payState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay-state", classOf[OrderEvent]))

        //==========================================================f4
        /**gcs:
          *receiptState 这个 state 是用来标志到账的流中是否已经收到了 5:36
          * */
    lazy val receiptState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt-state", classOf[ReceiptEvent]))

        //==========================================================f1
        /**gcs:
          * 这个 processElement1 是用来处理第一个流中的数据的
          * */
    // 订单支付事件数据的处理
    override def processElement1(pay: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
        //==========================================================f1.1
        /**gcs:
          * 订单事件来了之后我们会从到账事件中将状态提取出来，
          * 用来判断是否这个 key 中有到账事件。因为这两个流都是 keyBy()，所以每一个key理论上都会有一个 支付状态 payState和 到账状态 receiptState 。
          * */
      // 判断有没有对应的到账事件
      /**gcs:
        * 如果 PayState 已经到了，同时 receiptState 也到了。这时候是正常的，所以就把这条记录写到主流当中
        * */
      val receipt = receiptState.value()
      if( receipt != null ){
        // 如果已经有receipt，在主流输出匹配信息，清空状态
        out.collect((pay, receipt))
          //==========================================================f1.2
          /**gcs:
            * 同时把 receiptState 的状态进行更新
            * */
        receiptState.clear()
      } else {
          //==========================================================f1.3
          /**gcs:
            * 这时候，payState 的状态来了，但是 支付到账 receiptState 的状态还没有来，这时候我们就设置一个定时器，
            * 看 pay.eventTime * 1000L + 5000L 这条记录有没有来 19:40
            * 如果等到报警的时候，到账的状态还没有来，这时候就进行到账信息的报警
            * */
        // 如果还没到，那么把pay存入状态，并且注册一个定时器等待
        payState.update(pay)
        ctx.timerService().registerEventTimeTimer( pay.eventTime * 1000L + 5000L )
      }
    }

        //==========================================================f2
        /**gcs:
          * processElement2 函数是用来处理第二个被 connect 的流的每条数据的
          * 这是在处理 到账流的 receiptStream
          * */
    // 到账事件的处理
    override def processElement2(receipt: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      //==========================================================f2.1
        /**gcs:
          * 这时候到账的信息已经来了。我们需要判断的是 payState 有没有到
          * */
        // 同样的处理流程
      val pay = payState.value()

        //==========================================================f2.2
        /**gcs:
          * 如果发现 payState 已经到了。这时候就是 receiptState 和 payState 都到了的正常情况
          * */
      if( pay != null ){
          /**gcs:
            * 这时候就可以正常处理数据了
            * */
        out.collect((pay, receipt))
          /**gcs:
            * 清空 payState 状态
            * */
        payState.clear()
      } else {
          //==========================================================f2.3
          /**gcs:
            * 如果发现这个 key 的 payState还没有到。这时候的状态是 到账的信息到了，但是支付的信息还没有到，
            * 这时候我们就得
            * */
        receiptState.update(receipt)
          //==========================================================f2.4
          /**gcs:
            * 设定定时器，来等 receipt.eventTime * 1000L + 5000L 时间，看在这个时间段内，payState 会不会到
            * */
        ctx.timerService().registerEventTimeTimer( receipt.eventTime * 1000L + 5000L )
      }
    }

        //==========================================================f3
        /**gcs:
          * onTimer 函数，当这个函数被触发的时候，说明肯定有 payState 或者 receiptState 一方没有到的情况发生了
          * */
    override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      //==========================================================f3.1
        // 到时间了，如果还没有收到某个事件，那么输出报警信息
      if( payState.value() != null ){
          /**gcs:
            * 如果 payState 到了，但是 receiptState 没有到，这时候就可以把这条数据加入到标志为 receiptState 没有到的侧输出流中了
            * */
        // recipt没来，输出pay到侧输出流
        ctx.output(unmatchedPays, payState.value())
      }
      if( receiptState.value() != null ){
          //==========================================================f3.2
          /**gcs:
            * 如果是 receiptState 没有到，这时候就可以把数据加入到标志 payState 没有到的侧输出流当中了。
            * */
        ctx.output(unmatchedReceipts, receiptState.value())
      }
        //==========================================================f3.3
        /**gcs:
          * 同时最后清空 payState 状态和 receiptState
          * */
      payState.clear()
      receiptState.clear()
    }
  }
}
