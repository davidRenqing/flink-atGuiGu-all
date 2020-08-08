package com.atguigu.apitest

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.util.Random

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest
  * Version: 1.0
  *
  * Created by wushengran on 2019/9/17 10:11
  */

// 定义传感器数据样例类
case class SensorReading( id: String, timestamp: Long, temperature: Double )

object SourceTest {
  def main(args: Array[String]): Unit = {
      //==========================================================1
      /**gcs:
        * 创建环境
        * */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)



    // 4. 自定义数据源
    val stream4 = env.addSource(new SensorSource())

    // sink输出
    stream4.print("stream4").setParallelism(2)

    env.execute("source api test")
  }
}

class SensorSource() extends SourceFunction[SensorReading]{
  /**gcs:
    * 在这个类中我们要重写两个方法，
    * cancel 和 run 方法。
    * 这些 cancel 和 run 方法，会被 env.addSource 这个方法去调用，
    * */
  // 定义一个flag：表示数据源是否还在正常运行
  var running: Boolean = true

  //==========================================================f1
  /**gcs:
    * cancel 方法是代表不要再产生数据了。我们要定义一个哨兵，全局变量，一旦这个哨兵从True变成False 时，
    * 就代表不能再产生新的数据了
    * */
  override def cancel(): Unit = running = false

  //==========================================================f2
  /**gcs:
    * run 是用来新产生数据的逻辑的
    * */
  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {

    //==========================================================f3
    /**gcs:
      * 创建一个定时器
      * */
    // 创建一个随机数发生器
    val rand = new Random()

    //==========================================================f4
    /**gcs:
      * 随机产生10个传感器，并且为这20个传感器分配一个初始的时间。
      * 这里产生随机数的方法是使用 nextGaussian 函数。它是利用正项分布生成的真正的高斯随机数
      * rand.nextGaussian() 的值一般在 [-1,2] 之间
      * */
    // 随机初始换生成10个传感器的温度数据，之后在它基础随机波动生成流数据
    var curTemp = 1.to(10).map(
      i => ( "sensor_" + i, 60 + rand.nextGaussian() * 20 )
    )

    //==========================================================f5
    /**gcs:
      * 只要哨兵还为True，这时候就会继续产生数据
      * */
    // 无限循环生成流数据，除非被cancel
    while(running){
      //==========================================================f6
      /**gcs:
        * 更新这10个传感器的温度值
        * */
      // 更新温度值
      curTemp = curTemp.map(
        t => (t._1, t._2 + rand.nextGaussian())
      )
      //==========================================================f7
      /**gcs:
        * 获得当前的时间戳
        * */
      // 获取当前的时间戳
      val curTime = System.currentTimeMillis()
      //==========================================================f8
      /**gcs:
        * 将这10个传感器的温度都输出到 ctx 当中
        * */
      // 包装成SensorReading，输出
      curTemp.foreach(
        //==========================================================f9
        /**gcs:
          * 将我们这10个传感器的温度封装成一个 SensorReading 类。放到 ctx 当中。ctx 是上下文对象。可以获得这个流的上下文的信息
          * */
        t => ctx.collect( SensorReading(t._1, curTime, t._2) )
      )

      //==========================================================f10
      /**gcs:
        * 让我们的这个run函数睡 100 毫秒
        * */
      // 间隔100ms
      Thread.sleep(100)
    }
  }
}