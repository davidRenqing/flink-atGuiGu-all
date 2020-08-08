package com.atguigu.apitest

import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest
  * Version: 1.0
  *
  * Created by wushengran on 2019/8/24 11:16
  */
object SideOutputTest {
  def main(args: Array[String]): Unit = {
    //==========================================================1
    /**gcs:
      * 创建环境
      * */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.socketTextStream("localhost", 7777)

    //==========================================================2
    /**gcs:
      * 数组规整化，形成 sensorReading 的 dataStream
      * */
    val dataStream = stream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
        //==========================================================3
        /**gcs:
          * 注册 watermark 的生成周期，以及使用数据中的哪一个字段来作为 eventTime
          * */
      .assignTimestampsAndWatermarks( new BoundedOutOfOrdernessTimestampExtractor[SensorReading]( Time.seconds(1) ) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000
      } )

    //==========================================================4
    /**gcs:
      * 在 FreezingAlert 中将一个完整的 dataStream 分成两个流
      * */
    val processedStream = dataStream
      .process( new FreezingAlert() )


//    dataStream.print("input data")
    processedStream.print("processed data")
    //==========================================================5
    /**gcs:
      * 使用 getSideOutput 方法，来把侧输出流的 tag 提取出来
      * */
    processedStream.getSideOutput( new OutputTag[String]("freezing alert") ).print("alert data")


    env.execute("side output test")
  }
}

// 冰点报警，如果小于32F，输出报警信息到侧输出流
class FreezingAlert() extends ProcessFunction[SensorReading, SensorReading]{

//  lazy val alertOutput: OutputTag[String] = new OutputTag[String]( "freezing alert" )

  //==========================================================f1
  /**gcs:
    * 根据当前的温度进行判断。如果当前的温度小于 32.0 度，就会输出一个报警信息到侧输出流
    * */
  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    if( value.temperature < 32.0 ){
      //==========================================================f2
      /**gcs:
        * 使用 ctx.output 来顶一个一个 OutputTag 。这个 outputTag 就代表侧输出流的 tag
        * */
      ctx.output( new OutputTag[String]( "freezing alert" ), "freezing alert for " + value.id )
    }
    out.collect( value )
  }
}