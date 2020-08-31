package com.atguigu.orderpay_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.orderpay_detect
  * Version: 1.0
  *
  * Created by wushengran on 2019/9/25 10:27
  */
object OrderTimeoutWithoutCep {

  val orderTimeoutOutputTag = new OutputTag[OrderResult]("orderTimeout")

  def main(args: Array[String]): Unit = {
    //==========================================================1
    /**gcs:
      * 配置环境变量
      * */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //==========================================================2
    /**gcs:
      * 读取源文件
      * */
    // 读取订单数据
    val resource = getClass.getResource("/OrderLog.csv")
    //    val orderEventStream = env.readTextFile(resource.getPath)
    val orderEventStream = env.socketTextStream("localhost", 7777)
        //==========================================================3
        /**gcs:
          * 将数据进行规整化处理
          * */
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.orderId)

    //==========================================================4
    /**gcs:
      *
      * */
    // 定义process function进行超时检测
//    val timeoutWarningStream = orderEventStream.process( new OrderTimeoutWarning() )
    val orderResultStream = orderEventStream.process( new OrderPayMatch() )

    orderResultStream.print("payed")
    orderResultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")

    env.execute("order timeout without cep job")
  }


  class OrderPayMatch() extends KeyedProcessFunction[Long, OrderEvent, OrderResult]{

    //==========================================================f1
    /**gcs:
      * 设置一个 state 状态，标志我们的日志中是否有了 pay 的状态了。
      * */
    lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("ispayed-state", classOf[Boolean]))

    //==========================================================f2
    /**gcs:
      * 注册 timer 的状态
      * */
    // 保存定时器的时间戳为状态
    lazy val timerState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-state", classOf[Long]))

    //==========================================================f3
    /**gcs:
      * 执行 ProcessElement 函数的操作
      * */
    override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
      //==========================================================f3.1
      /**gcs:
        * 获取 isPayed
        * */
      // 先读取状态
      val isPayed = isPayedState.value()

      //==========================================================f3.2
      /**gcs:
        * 获取 timerState 的state
        * */
      val timerTs = timerState.value()

      //==========================================================f3.3
      /**gcs:
        * 如果来的这条日志的eventType 是 create 状态或者 Pay 状态做不同的判断
        * */
      // 根据事件的类型进行分类判断，做不同的处理逻辑
      if( value.eventType == "create" ){
        //==========================================================f3.4
        /**gcs:
          * 如果新来的日志的状态是 create，就判断 isPay 是否已经来过了。
          * 如果 isPayed 状态，说明这个 create 日志是迟到的日志。说明这条 create 日志本质上是早于 isPayed 日志创建的
          * */
        // 1. 如果是create事件，接下来判断pay是否来过
        if( isPayed ){
          //==========================================================f3.5
          /**gcs:
            * 这时候就可以指定，payed successfully
            * */
          // 1.1 如果已经pay过，匹配成功，输出主流，清空状态
          out.collect( OrderResult(value.orderId, "payed successfully") )

          /**gcs: 清空状态*/
          //==========================================================f3.6
          /**gcs:
            * 这时候就会删除我们的 eventTimer 定时器
            * */
          ctx.timerService().deleteEventTimeTimer(timerTs)

          //==========================================================f3.7
          /**gcs:
            * 清空状态
            * */
          isPayedState.clear()
          timerState.clear()
        } else {
          //==========================================================f3.8
          /**gcs:
            * 如果日志是 create 但是，用户还没有 payed 过
            * 这时候就会注册一个 15mins 的定时器。看 15mins 内 用户会不会 pay
            * */
          // 1.2 如果没有pay过，注册定时器等待pay的到来
          val ts = value.eventTime * 1000L + 15 * 60 * 1000L
          ctx.timerService().registerEventTimeTimer(ts)
          timerState.update(ts)
        }
      } else if ( value.eventType == "pay" ){
        //==========================================================f3.9
        // 2. 如果是pay事件，那么判断是否create过，用timer表示
        if( timerTs > 0 ){ /**gcs:如果有timerTs 定时器，说明之前有 create 日志来过*/

          // 2.1 如果有定时器，说明已经有create来过

          // 继续判断，是否超过了timeout时间
          if( timerTs > value.eventTime * 1000L ){
            //==========================================================f3.10
            /**gcs:
              * 如果 pay 这条日志来了，并且还有定时器，定时器也没有到期，这时候就说明在15mins 内完成了 create 订单 -> 支付订单 的过程。
              * 这时候就是一个匹配模式的正确过程
              * */
            // 2.1.1 如果定时器时间还没到，那么输出成功匹配
            out.collect( OrderResult(value.orderId, "payed successfully") )
          } else{
            //==========================================================f3.11
            /**gcs:
              * 否则就说明这个 pay日志距离 create 日志已经过了超过 15mins。这时候就应该输出侧输出流了。 payed timeout 了
              * */
            // 2.1.2 如果当前pay的时间已经超时，那么输出到侧输出流
            ctx.output(orderTimeoutOutputTag, OrderResult(value.orderId, "payed but already timeout"))
          }
          //==========================================================f3.12
          /**gcs:
            * 清除定时器
            * */
          // 输出结束，清空状态
          ctx.timerService().deleteEventTimeTimer(timerTs)
          //==========================================================f3.13
          /**gcs:
            * 删除 isPayedState 和 timerState state状态
            * */
          isPayedState.clear()
          timerState.clear()
        } else { /**gcs:在这种情况下，pay 先来了，但是之前没有 create。这时候就说明发生了数据乱序的情况了。*/
          //==========================================================f3.14
          /**gcs:
            * 更新 isPayedState状态。表示 pay日志已经来了
            * */
          // 2.2 pay先到了，更新状态，注册定时器等待create
          isPayedState.update(true)
          //==========================================================3.15
          /**gcs:
            * 注册一个 eventTimer。是为了等一等 eventType = create 的迟到的日志来。
            * 这里会注册一个 Timer 来等 create 日志。
            * 因为我们的 waterMark = value.eventTime * 1000L。所以我们就用 ctx.timerService().registerEventTimeTimer( waterMark )来注册 timer
            * 这时候 Timer 的值就是 waterMark。因为如果迟到的日志，迟到的时间大于 waterMark。如果迟到的日志应该被扔掉
            * 这里为什么要注册一个 eventTime 啊？
            * */
          ctx.timerService().registerEventTimeTimer( value.eventTime * 1000L )
          timerState.update(value.eventTime * 1000L)
        }
      }
    }

    /**gcs:
      * onTimer 函数如果别触发了。就说明一定有key来过了。因为只有有key来过了，这时候才会注册 timer
      * */
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
      // 根据状态的值，判断哪个数据没来
      if( isPayedState.value() ){
        // 如果为true，表示pay先到了，没等到create
        ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "already payed but not found create log"))
      } else{
        // 表示create到了，没等到pay
        ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "order timeout"))
      }
      isPayedState.clear()
      timerState.clear()
    }
  }
}



// 实现自定义的处理函数
class OrderTimeoutWarning() extends KeyedProcessFunction[Long, OrderEvent, OrderResult]{

  // 保存pay是否来过的状态
  lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("ispayed-state", classOf[Boolean]))

  override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
    // 先取出状态标识位
    val isPayed = isPayedState.value()

    if( value.eventType == "create" && !isPayed ){
      // 如果遇到了create事件，并且pay没有来过，注册定时器开始等待
      ctx.timerService().registerEventTimeTimer( value.eventTime * 1000L + 15 * 60 * 1000L )
    } else if( value.eventType == "pay" ){
      // 如果是pay事件，直接把状态改为true
      isPayedState.update(true)
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
    // 判断isPayed是否为true
    val isPayed = isPayedState.value()
    if(isPayed){
      out.collect( OrderResult( ctx.getCurrentKey, "order payed successfully" ) )
    } else {
      out.collect( OrderResult( ctx.getCurrentKey, "order timeout" ) )
    }
    // 清空状态
    isPayedState.clear()
  }

}
