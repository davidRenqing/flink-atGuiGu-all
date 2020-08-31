package com.atguigu.wc

import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.TimeUnit

import com.atguigu.apitest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}

import scala.util.Random

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.marketanalysis
  * Version: 1.0
  *
  * Created by wushengran on 2019/9/23 15:06
  */

// 输入数据样例类
/**gcs:
  * userId 是用户的ID
  * bevavior 是用户到底干了什么事？你是下载了，安装了，还是卸载了。
  * channel 即用户的这款 APP的下载安装渠道。用户是通过微博，微信，或者其他的方式下载安装的这款APP
  * timestamp 该事件的时间
  * */
case class MarketingUserBehavior( userId: String, behavior: String, channel: String, timestamp: Long )
// 输出结果样例类
case class MarketingViewCount( windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long ){
    override def toString: String = {
        s"windowStart:${windowStart}; windowEnd:${windowEnd}; channel:${channel}; behavior:${behavior}; count:${count}"
    }
}

object joinTest {
    def main(args: Array[String]): Unit = {
        //==========================================================1
        /**gcs:
          * 设置环境
          * */
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        //==========================================================2
        /**gcs:
          * 设置我们的 EventTime
          * */
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val source1 = env.addSource( new SimulatedEventSource() )
        val source2 = env.addSource( new SimulatedEventSource() )

        //==========================================================3
        /**gcs:
          * 为 source1 和 source2 设置EventTime 类型。
          * 因为这是顺序禅城的数据，所以没有数据延迟，不用指定waterMark
          * */
//        source1.assignAscendingTimestamps(elem =>elem.timestamp)
//        source2.assignAscendingTimestamps(elem => elem.timestamp)

        source1.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[MarketingUserBehavior]( Time.seconds(1) ) {
            override def extractTimestamp(element: MarketingUserBehavior): Long = element.timestamp
        })

        source2.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[MarketingUserBehavior]( Time.seconds(1) ) {
            override def extractTimestamp(element: MarketingUserBehavior): Long = element.timestamp
        })


        val result = source1.join(source2)
            .where(_.userId )
            .equalTo(_.userId)
            .window(SlidingEventTimeWindows.of(Time.minutes(10),Time.minutes(5)))
            .apply[String]((source1Data:MarketingUserBehavior,source2Data:MarketingUserBehavior) =>{
                s"source1: ${source1Data.userId} source2: ${source2Data.userId} source1Time:${source1Data.timestamp} source2Time:${source2Data.timestamp}"
            })



//        source1.join(source2).where[String](new source1Key())



        result.print("marketing join:")
        env.execute("app marketing by channel job")
    }
}

class source1Key extends KeySelector[MarketingUserBehavior,String]{
    override def getKey(value: MarketingUserBehavior): String = {
        value.userId
    }
}


//==========================================================3.1
// 自定义数据源
class SimulatedEventSource() extends RichSourceFunction[MarketingUserBehavior]{

    //==========================================================f1
    /**gcs:
      * 定义一个标志位。表示这个APP正在 running。
      * 当 running 为 true 时，我们就一直地产生数据。我们可以在 cancel 函数中定义 running=false
      * 这个 cancel
      * 我们会在 cancel 函数中指定 running=false。
      * */
    // 定义是否运行的标识位
    var running = true

    //==========================================================f2
    /**gcs:
      * 指定 用户行为的集合。用户是 CLICK DOWNLOAD INSTALL UNINSTALL 等操作
      * */
    // 定义用户行为的集合
    val behaviorTypes: Seq[String] = Seq("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL")

    //==========================================================f3
    /**gcs:
      * 指定渠道的集合
      * */
    // 定义渠道的集合
    val channelSets: Seq[String] = Seq("wechat", "weibo", "appstore", "huaweistore")

    //==========================================================f4
    /**gcs:
      * 定义一个随机数发生器，
      * */
    // 定义一个随机数发生器
    val rand: Random = new Random()

    //==========================================================f5
    /**gcs:
      * cancel 方法用来指定 running=false。
      * 这个函数由 flink 调用，当我们想要取消日志的产生时，我们就可以设定这个参数
      * */
    override def cancel(): Unit = running = false

    //==========================================================f6
    /**gcs:
      * run 函数
      * */
    override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
        // 定义一个生成数据的上限
        val maxElements = Long.MaxValue
        var count = 0L


        //==========================================================f6.1
        /**gcs:
          * 如果发现我们的标志位是在 running 而且我们当前的数据产生的上线小于 maxElements。
          * 这时候就会产出数据
          * */
        // 随机生成所有数据
        while( running && count < maxElements ){
            //==========================================================f6.2
            /**gcs:
              *随机产生 uuid
              * */
            val id = UUID.randomUUID().toString

            //==========================================================f6.3
            /**gcs:
              * 使用随机数，来随机产生一个 behavior
              * */
            val behavior = behaviorTypes(rand.nextInt(behaviorTypes.size))

            //==========================================================f6.4
            /**gcs:
              * 随机产生一个 channel
              * */
            val channel = channelSets(rand.nextInt(channelSets.size))

            //==========================================================f6.5
            /**gcs:
              * 日志的时间，就是我们当前的时间戳
              * */
            val ts = System.currentTimeMillis() + rand.nextLong()
            if (ts > Long.MinValue){
                println(s"-------------------------------------------------0")
                println(ts)
                //==========================================================f6.6
                /**gcs:
                  * 将我们产出的日志进行输出
                  * */
                ctx.collect( MarketingUserBehavior( id, behavior, channel, ts ) )

                //==========================================================f6.7
                /**gcs:
                  * 将当前的数据产出量增加1
                  * */
                count += 1
            }
            else {
                println(s"-------------------------------------------------1")
                println(ts)
            }

            //==========================================================f6.8
            /**gcs:
              * 产出了一条数据之后，让我们的程序睡 10L
              * */
            TimeUnit.MILLISECONDS.sleep(10L)
        }
    }
}