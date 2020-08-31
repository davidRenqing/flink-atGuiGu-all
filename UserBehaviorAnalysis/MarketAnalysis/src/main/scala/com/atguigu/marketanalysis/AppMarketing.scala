package com.atguigu.marketanalysis

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object AppMarketing {
  def main(args: Array[String]): Unit = {
    //==========================================================1
    /**gcs:
      * 创建执行环境
      * */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //==========================================================2
    /**gcs:
      * 指定数据的处理方式为 EventTime
      * */
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //==========================================================3
    /**gcs:
      * 增加Source
      * */
    val dataStream = env.addSource( new SimulatedEventSource() )
        //==========================================================4
        /**gcs:
          * 设定我们的 eventTime 为 日志的 timestamp
          * */
      .assignAscendingTimestamps(_.timestamp)
        //==========================================================5
        /**gcs:
          * 提取出没有安装的 behavior
          * */
      .filter( _.behavior != "UNINSTALL" )
        //==========================================================6
      .map( data => {
        ( "dummyKey", 1L )
      } )
      .keyBy(_._1)     // 以渠道和行为类型作为key分组
      .timeWindow( Time.hours(1), Time.seconds(10) )
        //==========================================================7
        /**gcs:
          * CountAgg 是对每一个window 当中的数据进行处理。
          * MarketingCountTotal 是对 window 的数据处理
          * */
      .aggregate( new CountAgg(), new MarketingCountTotal() )

    //==========================================================8
    /**gcs:
      * 打印 DStream 的输出
      * */
    dataStream.print()
    env.execute("app marketing job")
  }
}

//==========================================================7.1
/**gcs:
  * 每一个window 中每来一条数据，就会将计数器进行累加的操作
  * */
class CountAgg() extends AggregateFunction[(String, Long), Long, Long]{
  override def add(value: (String, Long), accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

//==========================================================7.2
/**gcs:
  * 对每一个window 进行操作
  * */
class MarketingCountTotal() extends WindowFunction[Long, MarketingViewCount, String, TimeWindow]{
  //==========================================================f1
  /**gcs:
    * 统计市场总量值
    * */
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[MarketingViewCount]): Unit = {
    val startTs = new Timestamp(window.getStart).toString
    val endTs = new Timestamp(window.getEnd).toString
    val count = input.iterator.next()
    out.collect( MarketingViewCount(startTs, endTs, "app marketing", "total", count) )
  }
}