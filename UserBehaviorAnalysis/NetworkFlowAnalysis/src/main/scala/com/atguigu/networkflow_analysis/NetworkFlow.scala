package com.atguigu.networkflow_analysis

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.networkflow_analysis
  * Version: 1.0
  *
  * Created by wushengran on 2019/9/23 9:21
  */

// 输入数据样例类
//==========================================================1
/**gcs:*
  * 创建一个 log 的case 类
  *
  */
case class ApacheLogEvent( ip: String, userId: String, eventTime: Long, method: String, url: String)

//==========================================================2
/**gcs:
  * 创建一个 UrlViewCount 的 case 类
  * */
// 窗口聚合结果样例类
case class UrlViewCount( url: String, windowEnd: Long, count: Long )

object NetworkFlow {
  def main(args: Array[String]): Unit = {
      //==========================================================3
      /**gcs:
        * 创建环境
        * */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

//    val dataStream = env.readTextFile("D:\\Projects\\BigData\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log")

      val properties = new Properties()
      properties.setProperty("bootstrap.servers", "localhost:9092")
      properties.setProperty("group.id", "consumer-group-network")
      properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      properties.setProperty("auto.offset.reset", "earliest")

      val sourceStream = env.addSource( new FlinkKafkaConsumer[String]("netWorkFlow", new SimpleStringSchema(), properties) )

//      val sourceStream = env.socketTextStream("localhost", 7777)

    val dataStream = sourceStream
      .map( data => {
          //==========================================================4
          /**gcs:
            * 数据按照空格进行分隔
            * */
        val dataArray = data.split(" ")

          //==========================================================5
          /**gcs:
            * 根据日期的格式，对日志中的时间进行解析
            * */
        // 定义时间转换
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
          //==========================================================6
          /**gcs:
            * 获得时间戳
            * */
        val timestamp = simpleDateFormat.parse(dataArray(3).trim).getTime
          //==========================================================7
          /**gcs:
            * 日志封装
            * */
        ApacheLogEvent( dataArray(0).trim, dataArray(1).trim, timestamp, dataArray(5).trim, dataArray(6).trim )
      } )
        //==========================================================8
        /**gcs:
          * 因为日志是乱序的，所以我们要指定乱序情况下的 waterMark
          * */
      .assignTimestampsAndWatermarks( new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
        override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime
      } )
        //==========================================================9
        /**gcs:
          * 进行 keyBy 的操作
          * */
      .keyBy(_.url)
        //==========================================================10
        /**gcs:
          * 进行分窗口的操作
          * */
      .timeWindow(Time.minutes(10), Time.seconds(5))
        //==========================================================11
        /**gcs:*/
      .allowedLateness(Time.seconds(60))
      .aggregate( new CountAgg(), new WindowResult() )

      //==========================================================12
      /**gcs:
        * 按照 windowEnd 进行 keyBy 的操作
        * */
    val processedStream = dataStream
      .keyBy(_.windowEnd)
        //==========================================================13
        /**gcs:
          * 统计出 top5 的URL
          * */
      .process( new TopNHotUrls(5) )

    dataStream.print("aggregate")
    processedStream.print("process")

    env.execute("network flow job")
  }
}

//==========================================================11.1
/**gcs:
  * 来一条元素进行一次数据聚合的操作
  * */
// 自定义预聚合函数
class CountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long]{
    //==========================================================f1
    /**gcs:
      * 进行数据的累加
      * */
  override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

    //==========================================================f2
    /**gcs:
      * 创建一个 accumulator
      * */
  override def createAccumulator(): Long = 0L

    //==========================================================f3
    /**gcs:
      * 获得最终的 accumulator 的累加结果
      * */
  override def getResult(accumulator: Long): Long = accumulator

    //==========================================================f4
    /**gcs:
      * merge 两个 accumulator
      * */
  override def merge(a: Long, b: Long): Long = a + b
}

//==========================================================11.2
/**gcs:
  * 当 window 窗口时间到了，就会执行这个 windowFunction
  * */
// 自定义窗口处理函数
class WindowResult() extends WindowFunction[Long, UrlViewCount, String, TimeWindow]{

    //==========================================================f1
    /**gcs:
      * 执行window 窗口函数，将我们在 11.1 根据 key 聚合完成的数据进行封装
      * */
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect( UrlViewCount( key, window.getEnd, input.iterator.next() ) )
  }
}

//==========================================================13
// 自定义排序输出处理函数
class TopNHotUrls(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String]{
    //==========================================================f1
    /**gcs:
      * 建立一个Map State
      * */
  lazy val urlState: MapState[String, Long] = getRuntimeContext.getMapState( new MapStateDescriptor[String, Long]("url-state", classOf[String], classOf[Long] ) )

    //==========================================================f2
    /**gcs:
      * 每来一条数据，就会执行这个 processElement 的函数
      * */
  override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
      //==========================================================f3
      /**gcs:
        * 将 url，count，放到 urlState 当中
        * */
    urlState.put(value.url, value.count)
      //==========================================================f4
      /**gcs:
        * 设置一个 eventTime的 timer。
        * 当window窗口到达之后，触发这个 Timer
        * */
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

    //==========================================================f5
    /**gcs:
      * onTimer 函数
      *
      * */
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      //==========================================================f1
      /**gcs:
        * 将 urlState 中的 allUrlView 当拿出来。
        * 这里的 urlViews 是否可以用一个Set 来表示呢？
        * */
    // 从状态中拿到数据
    val allUrlViews: ListBuffer[(String, Long)] = new ListBuffer[(String, Long)]()
    val iter = urlState.entries().iterator()
      //==========================================================f2
      /**gcs:
        * 遍历每一个迭代器，将所有的元素都放到 allUrlViews 当中
        * */
    while(iter.hasNext){
      val entry = iter.next()
      allUrlViews += (( entry.getKey, entry.getValue ))
    }

//    urlState.clear()

      //==========================================================f3
      /**gcs:
        * 将我们的Url 进行排序，之后取出 topSize 的URL
        * */
    val sortedUrlViews = allUrlViews.sortWith(_._2 > _._2).take(topSize)

      //==========================================================f4
      /**gcs:
        * 格式化输出结果
        * */
    // 格式化结果输出
    val result: StringBuilder = new StringBuilder()
    result.append("时间：").append( new Timestamp( timestamp - 1 ) ).append("\n")
    for( i <- sortedUrlViews.indices ){
      val currentUrlView = sortedUrlViews(i)
      result.append("NO").append(i + 1).append(":")
        .append(" URL=").append(currentUrlView._1)
        .append(" 访问量=").append(currentUrlView._2).append("\n")
    }
    result.append("=============================")
//    Thread.sleep(1000)
    out.collect(result.toString())
  }
}