package com.atguigu.apitest

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**gcs:
  * 视频006
  *
  * */
object WindowTest {
  def main(args: Array[String]): Unit = {
    //==========================================================1
    /**gcs:
      * 创建一个flink的环境变量。Flink 会根据我们的执行环境，动态地解析我们的Flink环境是本地的环境，还是远程的环境
      * */
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //==========================================================2
    /**gcs:
      * 设置默认的并行度为1
      * */
    env.setParallelism(1)

    //==========================================================3
    /**gcs:
      * 设置我们的事件的时间为 EventTime 的时间
      * */
    // 设置事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(500)

    // 读入数据
    //    val inputStream = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt")
/**gcs:
      * 视频006
      *之后从 7777 端口中提取数据
      * */
    val inputStream = env.socketTextStream("localhost", 7777)



    //==========================================================4
    /**gcs:
      * 将我的数据按照 逗号 进行分隔，
      * 之后拼接成 SensorReading 的case类
      * */
    val dataStream = inputStream
      .map(
        data => {
          val dataArray = data.split(",")
          SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
        }
      )


      dataStream.keyBy(_.id)
              .process(new MyProcess())

      //==========================================================
      /**gcs:
        * keyedProcessFunction需要提供三个参数。[keyType,inType,outType]
        * keyType 是我们的keyBy 算子使用的 _.id 的类型。在这里就是 String 的类型。因为我们在使用 keyBy(_.id)，这种方式返回的key是 String 类型
        * inType 是 keyBy() 算子之后，我们的value 的类型
        * outType 是我们处理完数据之后的输出结果的类型。这里我们也定义为 String类型
        * */
      class MyProcess() extends KeyedProcessFunction[String,SensorReading,String]{
          override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
              //==========================================================1
              ctx.timerService().registerEventTimeTimer(2000)
          }
      }


        //==========================================================5
        /**gcs:
          * 指定我们的 waterMark 和 哪些字段来作为 EventTime.
          * 当我们的日志是已经有序的日志了。这时候我们就不需要使用 watermark了。这时候，我们就可以直接使用 assignAscendingTimestamps 方法
          * 来确定我们日志中的哪一个字段充当 eventTime。例子在这个例子中，我们使用日志中的 _.timestamp 来充当 eventTime
          * */
//        .assignAscendingTimestamps(_.timestamp * 1000L)
        //==========================================================5.1
        /**gcs:
          * 现在比如我们的日志是乱序的日志。我们指定watermark 的方式就是使用
          * BoundedOutOfOrdernessTimestampExtractor 这个类。
          * 这个类中的 extractTimestamp 方法就是用每一条最新的日志来生成一个时间 T1。这个时间会被用于接下来生成 watermark 的
          *之后 waterMark = T1 - 指定的偏移时间量，Time.seconds(1)
          * watermark 代表的含义是，所有的时间戳小于它的，都已经到来了。如果有一个日志的时间戳大于watermark，那就代表这个日志已经过期了，可以被丢掉了。
          * */
//      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
//      override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
//    })
//      .assignTimestampsAndWatermarks( new MyAssigner() )
//        //==========================================================6
//        /**gcs:
//          * 之后进行 map 操作 和 keyBy 的操作
//          * */
//      .map(data => (data.id, data.temperature))
//      .keyBy(_._1)
////        .process( new MyProcess() )
//      .timeWindow(Time.seconds(10), Time.seconds(3))
//      .reduce((result, data) => (data._1, result._2.min(data._2))) // 统计10秒内的最低温度值



    dataStream.print()

    env.execute("window api test")
  }
}

//==========================================================
/**gcs:
  * 使用自定义的 Assigner 来定义 watermark 的产生时间
  * */
class MyAssigner() extends AssignerWithPeriodicWatermarks[SensorReading]{
    //==========================================================5
    /**gcs:
      * 这是一个阈值 bound =N，表示我们允许里最大日志时间戳 maxTs 少 N 时间的迟到日志过来，并进入到窗口，参与运算
      * */
  // 定义固定延迟为3秒
  val bound: Long = 3 * 1000L
    //==========================================================2
    /**gcs:
      * 我们定义一个变量，是一个全局变量。这个变量中存储着 extractTimestamp 中更新的时间。
      * 这个存储着时间的变量 maxTs 一会儿就会被用来计算 watermark
      * */
  // 定义当前收到的最大的时间戳
  var maxTs: Long = Long.MinValue

    //==========================================================3
    /**gcs:
      * 这个函数会被定期地执行，来生成一个 watermark
      * */
  override def getCurrentWatermark: Watermark = {
      //==========================================================4
      /**gcs:
        * 我们可以看到。我们使用当前的 maxTs 的时间，减去一个固定的阈值 bound，来指定我们的 watermark
        * */
    new Watermark(maxTs - bound)
  }

    //==========================================================1
    /**gcs:
      * extractTimestamp 这个函数，每来一条日志就会被调用一次。来更新一会儿定期生成 watermark 时所用到的时间。
      * 通常这个时间是在当前流中的所有日志中的时间戳的最大值。
      * */
  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {

    maxTs = maxTs.max(element.timestamp * 1000L)
    element.timestamp * 1000L
  }
}

//==========================================================
/**gcs:
  * 这种方式可以让不仅仅按照时间周期来定期地产生 watermark。
  * 这种允许我们可以根据日志的类型来产生watermark。
  * */
class MyAssigner2() extends AssignerWithPunctuatedWatermarks[SensorReading]{
  val bound: Long = 1000L


    //==========================================================2
    /**gcs:
      * 这个函数会在 extractTimestamp 函数执行完之后运行。extractTimestamp函数在产生用于生成 watermark 的时间之后，
      * 会将这个时间作为一个形参 extractedTimestamp: Long 传递给 checkAndGetNextWatermark 这个函数，
      * 这个 checkAndGetNextWatermark 函数也会来一条日志就运行一次。它可以用来根据日志的类型，来判断产不产生日志
      * */
  override def checkAndGetNextWatermark(lastElement: SensorReading, extractedTimestamp: Long): Watermark = {
      //==========================================================3
      /**gcs:
        * 比如，这里我们就可以判断我们的日志是不是 sensor_1 的日志。
        * 如果我们的日志是来自 sensor_1，这时候，我们就会生成一个watermark。而不用必须按照时间周期来产生watermark
        * */
    if( lastElement.id == "sensor_1" ){
      new Watermark(extractedTimestamp - bound)
    }else{
      null
    }
  }
    //==========================================================1
    /**gcs:
      * 这个函数，还是每来一条新日志，就会被执行一次。来更新使用哪一个时间来生成 watermark
      * 之后这个函数生成的时间，会传递给 checkAndGetNextWatermark 这个函数中的 extractedTimestamp 参数
      * */
  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    element.timestamp * 1000L
  }
}
