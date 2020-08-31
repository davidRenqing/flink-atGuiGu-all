package com.atguigu.orderpay_detect

import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

//==========================================================1
/**gcs:
  * 定义一个 order的事件信息
  * */
// 定义输入订单事件的样例类
case class OrderEvent(orderId: Long, eventType: String, txId: String, eventTime: Long)

// 定义输出结果样例类
case class OrderResult(orderId: Long, resultMsg: String)

object OrderTimeout {
  def main(args: Array[String]): Unit = {
    //==========================================================1
    /**gcs:
      * 设置环境变量
      * */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //==========================================================2
    /**gcs:
      * 设置并行度
      * */
    env.setParallelism(1)

    //==========================================================3
    /**gcs:
      * 读取 order 的信息
      * */
    // 1. 读取订单数据
//    val resource = getClass.getResource("/OrderLog.csv")
    //    val orderEventStream = env.readTextFile(resource.getPath)

    val sourceStream = env.readTextFile("/Users/congshuo.geng/Documents/david/flink-尚硅谷/3.代码/flink-atGuiGu-all/UserBehaviorAnalysis/OrderPayDetect/src/main/resources/OrderLog.csv")

//    val sourceStream = env.socketTextStream("localhost", 7777)
    //==========================================================4
    /**gcs:
      * 拼接 sourceStream, 包装样例类
      * */
    val orderEventStream = sourceStream
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.orderId)

    //==========================================================5
    /**gcs:
      * 定义一个 复杂的模式
      * 这个复杂模式，begin 是 create 订单；之后第二步使用 followedBy 方法，定义用户的 eventType 等于 pay。followBy 允许中间有其他的事件，
      * 但是
      * 要求用户在 15mins 完成之后模式。如果用户在 15mins没有完成这个模式，这时候就算这个模式匹配失败了。
      */
    // 2. 定义一个匹配模式
    val orderPayPattern = Pattern.begin[OrderEvent]("begin").where(_.eventType == "create")
      .followedBy("follow").where(_.eventType == "pay")
      .within(Time.minutes(15))


    //==========================================================6
    /**gcs:
      * 将模式应用到 source Stream中
      * */
    // 3. 把模式应用到stream上，得到一个pattern stream
    val patternStream = CEP.pattern(orderEventStream, orderPayPattern)

    //==========================================================6
    /**gcs:
      * 创建一个 侧输入流 ，将已经部分符合复杂模式了，但是没有完全匹配完的序列进行侧输出
      * */
    val orderTimeoutOutputTag = new OutputTag[OrderResult]("orderTimeout")

    //==========================================================7
    /**gcs:
      * 这个 slect 方法支持传入 3 个参数，
      * outputTag: OutputTag[L],
      * patternTimeoutFunction: PatternTimeoutFunction[T, L],
      * patternSelectFunction: PatternSelectFunction[T, R]
      *
      * patternTimeoutFunction 函数是用来处理 有部分符合模式，但是没有全部符合模式的匹配失败的记录组合
      * patternSelectFunction 函数是用来提取符合模式的记录组合
      * */
    // 4. 调用select方法，提取事件序列，超时的事件要做报警提示
    val resultStream = patternStream.select(orderTimeoutOutputTag,
      new OrderTimeoutSelect(),
      new OrderPaySelect())

    resultStream.print("payed")
    resultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")

    env.execute("order timeout job")
  }
}

//==========================================================7.1
/**gcs:
  * 处理不符合模式的数据
  * */
// 自定义超时事件序列处理函数
class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult] {
  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
    //==========================================================f1
    /**gcs:
      * 将订单建立，但是在 15 mins 内没有进行支付的记录提取出来
      * */
    val timeoutOrderId = map.get("begin").iterator().next().orderId

    //==========================================================f2
    /**gcs:
      * 进行输出
      * */
    OrderResult(timeoutOrderId, "timeout")
  }
}

//==========================================================7.2
/**gcs:
  * 这种方式是定义符合模式的例子。即用户在创建了一个订单之后，在15mins 内完成了交付。完全匹配了这个模式
  * */
// 自定义正常支付事件序列处理函数
class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderResult] {
  //==========================================================f1
  /**gcs:
    * 创建 select 方法
    * */
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    /**gcs:
      * 将符合模式的记录提取出来
      * */
    val payedOrderId = map.get("follow").iterator().next().orderId
    /**gcs:
      * 输出 payed successfully
      * */
    OrderResult(payedOrderId, "payed successfully")
  }
}
