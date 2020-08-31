package com.atguigu.marketanalysis

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.marketanalysis
  * Version: 1.0
  *
  * Created by wushengran on 2019/9/24 10:10
  */
// 输入的广告点击事件样例类
case class AdClickEvent( userId: Long, adId: Long, province: String, city: String, timestamp: Long )
// 按照省份统计的输出结果样例类
case class CountByProvince( windowEnd: String, province: String, count: Long )
// 输出的黑名单报警信息
case class BlackListWarning( userId: Long, adId: Long, msg: String )

object AdStatisticsByGeo {
  // 定义侧输出流的tag
  val blackListOutputTag: OutputTag[BlackListWarning] = new OutputTag[BlackListWarning]("blackList")

  def main(args: Array[String]): Unit = {
    //==========================================================1
    /**gcs:
      * 创建执行环境
      * */
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //==========================================================2
    /**gcs:
      * 使用 eventTime
      * */
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //==========================================================3
    /**gcs:
      * 设置并行度为 1
      * */
    env.setParallelism(1)

    //==========================================================4
    /**gcs:
      * 读取源数据的路径
      * */
    // 读取数据并转换成AdClickEvent
//    val resource = getClass.getResource("/AdClickLog.csv")
    //==========================================================5
    /**gcs:
      * 读取文件
      * */
//      val sourceStream = env.readTextFile(resource.getPath)
    val sourceStream = env.readTextFile("/Users/congshuo.geng/Documents/david/flink-尚硅谷/3.代码/flink-atGuiGu-all/UserBehaviorAnalysis/MarketAnalysis/src/main/resources/AdClickLog.csv")
    val adEventStream = sourceStream
        //==========================================================6
        /**gcs:
          * 处理源数据，将数据规整化
          * */
      .map( data => {
        val dataArray = data.split(",")
        AdClickEvent( dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim, dataArray(3).trim, dataArray(4).trim.toLong )
      } )
        //==========================================================7
        /**gcs:
          * 设置 日志中的 timestamp 为 EventTime。而且通过观察，我们的日志没有明显的延迟情况，所以在这里就不再做waterMark的设定了
          * */
      .assignAscendingTimestamps(_.timestamp * 1000L)

    //==========================================================11
    /**gcs:
      * 对于广告刷单
      * 按照 userId 和 adId 广告ID 对数据进行 keyBy 的操作
      * */
    // 自定义process function，过滤大量刷点击的行为
    val filterBlackListStream = adEventStream
      .keyBy( data => (data.userId, data.adId) )
        //==========================================================12
        /**gcs:
          * 过滤单日广告点击量超过 100 次的，我们就把该用户设置为当天的黑名单。
          * 进入到黑名单的用户，当天的广告统计就不参与了。该用户会被放在黑名单一天
          * */
      .process( new FilterBlackListUser(100) )

    //==========================================================8
    /**gcs:
      * 按照 省份进行keyBy
      * */
    // 根据省份做分组，开窗聚合
    val adCountStream = filterBlackListStream
      .keyBy(_.province)
        //==========================================================9
      .timeWindow( Time.hours(1), Time.seconds(5) )
        //==========================================================10
      .aggregate( new AdCountAgg(), new AdCountResult() )

    //==========================================================13
    /**gcs:
      * 输出 按照省份进行 keyed 操作之后的广告点击量的统计结果
      * */
    adCountStream.print("adCount")

    //==========================================================14
    /**gcs:
      * 输出当天的广告黑名单的统计结果
      * */
    filterBlackListStream.getSideOutput(blackListOutputTag).print("blacklist")

    //==========================================================15
    /**gcs:
      * 开始运行程序
      * */
    env.execute("ad statistics job")
  }

  //==========================================================12.1
  /**gcs:
    *对单日广告点击量大于100 的用户进行filter 过滤
    * */
  class FilterBlackListUser(maxCount: Int) extends KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]{

    //==========================================================f1
    /**gcs:
      * 这个是 keyed state。因为我们 11 中指定的key是 (userId,adId)，所以我们 每一个user的每一个广告，就会有一个 keyedState。
      * 即这个 countState 就代表同一用户点击同一广告 的数量。如果countState 大于 maxCount 就把该用户在当天记入到黑名单中
      * */
    // 定义状态，保存当前用户对当前广告的点击量
    lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count-state", classOf[Long]))

    //==========================================================f2
    /**gcs:
      * 这也是一个 keyed state，当 (userId,adId) 为 true 时，代表该用户已经处于黑名单中。如果为 false 时，代表该用户还没有处于黑名单中
      * */
    // 保存是否发送过黑名单的状态
    lazy val isSentBlackList: ValueState[Boolean] = getRuntimeContext.getState( new ValueStateDescriptor[Boolean]("issent-state", classOf[Boolean]) )

    //==========================================================f3
    /**gcs:
      * 定义一个定时器
      * */
    // 保存定时器触发的时间戳
    lazy val resetTimer: ValueState[Long] = getRuntimeContext.getState( new ValueStateDescriptor[Long]("resettime-state", classOf[Long]) )

    //==========================================================f4
    /**gcs:
      * 每来一条数据就会处理一次。
      * */
    override def processElement(value: AdClickEvent, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#Context, out: Collector[AdClickEvent]): Unit = {

      //==========================================================f4.1
      /**gcs:
        * 取出该 (userId,adId) 组成的key 的 当前的 count 值
        * */
      // 取出count状态
      val curCount = countState.value()

      //==========================================================f4.2
      /**gcs:
        * 如果我们是第一次处理数据，这时候就会注册一个定时器，每天 00：00 开始出发一次定时器 Timer() 函数中的逻辑
        * */
      // 如果是第一次处理，注册定时器，每天00：00触发
      if( curCount == 0 ){
        val ts = ( ctx.timerService().currentProcessingTime()/(1000*60*60*24) + 1) * (1000*60*60*24)
        resetTimer.update(ts)
        ctx.timerService().registerProcessingTimeTimer(ts)
      }

      //==========================================================f4.3
      /**gcs:
        * 如果发现 (userId,adId) 的当前的点击量已经大于 maxCount
        * */
      // 判断计数是否达到上限，如果到达则加入黑名单
      if( curCount >= maxCount ){
        //==========================================================f4.4
        /**gcs:
          * 判断该 (userId,adId) 是否在黑名单中。即该用户有没有点击adId 广告超过了 100 次
          * */
        // 判断是否发送过黑名单，只发送一次
        if( !isSentBlackList.value() ){
          //==========================================================f4.5
          /**gcs:
            * 如果该用户点击同一个广告超过了 maxCount。但是该用户还没有处于黑名单中，这时候就会把该用户设置为 false
            * */
          isSentBlackList.update(true)

          //==========================================================f4.6
          /**gcs:
            * 把该用户放到侧边流当中
            * */
          // 输出到侧输出流
          ctx.output( blackListOutputTag, BlackListWarning(value.userId, value.adId, "Click over " + maxCount + " times today.") )
        }
        return
      }

      //==========================================================f4.7
      /**gcs:
        * 计数器进行加 1 的操作。表示 (userId,adId)，该 userId 对于同一个 adId 又多了一次访问
        * */
      // 计数状态加1，输出数据到主流
      countState.update( curCount + 1 )
      out.collect( value )
    }

    //==========================================================f5
    /**gcs:
      * Timer 函数触发操作。这个函数当定时器到时间之后，就会触发
      * */
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]): Unit = {

      //==========================================================f5.1
      /**gcs:
        * 一旦到了定时器触发的时候，就会清空我们的定时器
        * */
      // 定时器触发时，清空状态
      if( timestamp == resetTimer.value() ){
        //==========================================================f5.2
        /**gcs:
          * 清空 isSentBlackList, countState, resetTimer 定时器中的内容
          * */
        isSentBlackList.clear()
        countState.clear()
        resetTimer.clear()
      }
    }
  }
}

//==========================================================10.1
// 自定义预聚合函数
class AdCountAgg() extends AggregateFunction[AdClickEvent, Long, Long]{
  override def add(value: AdClickEvent, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

//==========================================================10.2
/**gcs:
  * 每一个省份的广告点击量的分析
  * */
// 自定义窗口处理函数
class AdCountResult() extends WindowFunction[Long, CountByProvince, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
    out.collect( CountByProvince( new Timestamp(window.getEnd).toString, key, input.iterator.next() ) )
  }
}
