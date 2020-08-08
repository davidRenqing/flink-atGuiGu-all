package com.atguigu.apitest

import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
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
  * Created by wushengran on 2019/8/24 10:14
  */
object ProcessFunctionTest {
  def main(args: Array[String]): Unit = {
    //==========================================================1
    /**gcs:
      * 获得环境变量
      * */
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //==========================================================2
    /**gcs:
      * 设置总体的并行度
      * */
    env.setParallelism(1)

    //==========================================================3
    /**gcs:
      * 设置时间为 EventTime
      * */
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //==========================================================2
    /**gcs:
      * 设定 checkpoint 的时间间隔。每隔 60000 毫秒，即每隔 60s 就会发送一次 checkpoint 的日志
      * */
    env.enableCheckpointing(60000)

    //==========================================================3
    /**gcs:
      * 设置checkpoint 的模式，至少发送一次 checkpoint 的周期。至少发送一次 checkpoint.
      * 这种方式用于，当 checkpoint 发送给并行度的task失败的时候，这时候就会再次发送一次 checkpoint 日志
      * */
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)

    //==========================================================4
    /**gcs:
      * 设置这个checkpoint过了 100000，然后 checkpoint 日志还没有被发送成功。这时候，这个 checkpoint 点就会被丢弃掉了。
      * */
    env.getCheckpointConfig.setCheckpointTimeout(100000)

    //==========================================================5
    /**gcs:
      * 设定当checkpoint失败之后，才去什么样的措施。
      * 如果这个函数的参数被设定为 True，当checkpoint 失败之后，整个任务就会失败。
      * 如果这个函数的参数被设定为 False，当 checkpoint 失败之后，就会自动地跳过本次的 checkpoint，之后继续运行我们的程序
      * */
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)

    //==========================================================6
    /**gcs:
      *我们是每隔一段时间，假设为 K s 进行一次checkpoint。
      * 我们的 checkpoint 的持续时间是 N
      * 可能存在 N > K 的情况。这样就会发生在这一轮的checkpoint 没有完成，下一轮就出现的情况，这样就会出现多个checkpoint 同时在同一时刻运行的时候
      * setMaxConcurrentCheckpoints 设置的是做多的在同一时刻同时运行 checkpoint 的数目，默认值是1
      * */
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    //==========================================================7
    /**gcs:
      * 两个 checkpoint 之间的最小的时间间隔。
      * 即，如果我们当前的 checkpint 的数据小于 env.getCheckpointConfig.setMaxConcurrentCheckpoints(N) 中的N个checkpoint 的数目
      * 这时候我就每隔 env.getCheckpointConfig.setMinPauseBetweenCheckpoints(K) K时间再启动一个checkpoint。
      * */
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(100)

    //==========================================================8
    /**gcs:
      *开启checkpoint 的外部的持久化。
      * 如果我们的任务失败了，checkpoint默认会被自动清理掉的。如果我们通过这个代码设置 ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION
      * 那当我们的任务停掉或者失败掉之后，我们的 checkpoint就会被清理掉。
      * 但是如果我们设置 ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION 那当我们的任务失败了之后，我们该任务做的 checkpoint 是不会被清理掉的
      * */
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)


    //==========================================================9
    /**gcs:
      * 根据失败率进行重启。
      * 如果我的任务每隔<=300s就失败一次，而且我每隔 10s 就尝试启动一次这个失败的任务，尝试了3次，这个任务还是每隔<=300s 就 失败一次。这时候我就任务不再做尝试了。
      * 你比如说，现在我有一个任务 application-1 运行失败了，我flink自动把这个任务给启动起来了，但是还没到 300s 这个任务又运行失败了。okey，我flink 继续再启动起来这个任务
      * 但是这个任务又在不超过300s中又挂掉了。这样连续持续了3次。好了我就认为这个任务真的挂掉了。
      * 那比方说我现在已经尝试了2次了，前两次这个任务都是用不超过300s的时间就挂掉了，但是当我尝试第3次是，这个任务在 500s 挂了，那我就前3次的记录就清零了，我再尝试3次，看你是不是
      * 在300s 内挂掉了。我一直尝试，直到你连续3次在不超过300s的时间内挂掉了。就算这个任务真的挂掉了
      * */
    env.setRestartStrategy(RestartStrategies.failureRateRestart(3,
      org.apache.flink.api.common.time.Time.seconds(300),
      org.apache.flink.api.common.time.Time.seconds(10)))


    //==========================================================10
    /**gcs:
      * 这种方法是指定我的任务在失败之后，自动重启3次，每次的间隔是 500ms
      * */
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,500))


    //==========================================================7
    /**gcs:
      * 定义从 socket中读取内容
      * */
//    env.setStateBackend( new RocksDBStateBackend("") )

    val stream = env.socketTextStream("localhost", 7777)

    //==========================================================8
    /**gcs:
      * 数据整理
      * */
    val dataStream = stream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
        //==========================================================9
        /**gcs:
          * 设定 watermark
          * */
      .assignTimestampsAndWatermarks( new BoundedOutOfOrdernessTimestampExtractor[SensorReading]( Time.seconds(1) ) {
      override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000
    } )

    //==========================================================10
    /**gcs:
      *指定我们的 process 函数
      * */
    val processedStream = dataStream.keyBy(_.id)
      .process( new TempIncreAlert() )

    val processedStream2 = dataStream.keyBy(_.id)
//      .process( new TempChangeAlert(10.0) )
      .flatMap( new TempChangeAlert(10.0) )

    val processedStream3 = dataStream.keyBy(_.id)
      .flatMapWithState[(String, Double, Double), Double]{
      //==========================================================f1
      // 如果没有状态的话，也就是没有数据来过，那么就将当前数据温度值存入状态
      /**gcs:
        * 这里的 None 就代表此时我的 state 是空。之后返回值就是把state设置为了 Some(input.temerature)
        * */
      case ( input: SensorReading, None ) => ( List.empty, Some(input.temperature) )
        //==========================================================f2
      // 如果有状态，就应该与上次的温度值比较差值，如果大于阈值就输出报警
      case ( input: SensorReading, lastTemp: Some[Double] ) =>
        val diff = ( input.temperature - lastTemp.get ).abs
        //==========================================================f3
        /**gcs:
          * 如果发现当前的数据的温度要比之前的温度的值相差超过 10.0
          * */
        if( diff > 10.0 ){
          //==========================================================f4
          /**gcs:
            * 如果发现我们的当前的温度和之前的温度相差超过10度，这时候就输出报警，
            * 一定要注意这里要输出一个 list 二元组。
            * */
          ( List((input.id, lastTemp.get, input.temperature)), Some(input.temperature) )
        } else
        //==========================================================f5
        /**gcs:
          * 如果温度相差小于10.这时候就输出空
          * */
          ( List.empty, Some(input.temperature) )
    }

    dataStream.print("input data")
    processedStream3.print("processed data")

    processedStream.print()

    env.execute("process function test")
  }
}

/**gcs:
  * 我们要对比当前接收到的日志和上一条日志的大小，
  * 所以，需要我们保存下来当前的 state 状态，这里就用到了状态存储
  * */
class TempIncreAlert() extends KeyedProcessFunction[String, SensorReading, String]{

  //==========================================================f1
  /**gcs:
    * 设定一个状态，来存储上一个state 状态的值
    * */
  // 定义一个状态，用来保存上一个数据的温度值
  lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState( new ValueStateDescriptor[Double]("lastTemp", classOf[Double]) )
  // 定义一个状态，用来保存定时器的时间戳
  lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState( new ValueStateDescriptor[Long]("currentTimer", classOf[Long]) )

  //==========================================================f2
  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {

    //==========================================================f3
    /**gcs:来一条新数据，要先把上一个状态的时间提取出来*/
    // 先取出上一个温度值
    val preTemp = lastTemp.value()

    //==========================================================f4
    /**gcs:
      * 同时更新时间
      * */
    // 更新温度值
    lastTemp.update( value.temperature )

    //==========================================================f8
    /**gcs:
      * 注册一个存储当前的定时器时间的状态。用于存储 f8 的状态
      * */
    val curTimerTs = currentTimer.value()

    if( value.temperature < preTemp || preTemp == 0.0 ){
      //==========================================================f9
      /**gcs:
        * 如果发现温度下降了，或是第一条日志，这时候如果之前定义了 定时器，这时候就要把定时器给删除，
        * 否则就会出现，即使温度下降了，因为之前设置了定时器，这时候也仍然会发生报警
        * */
      // 如果温度下降，或是第一条数据，删除定时器并清空状态
      ctx.timerService().deleteProcessingTimeTimer( curTimerTs )
      currentTimer.clear()
    } else if ( value.temperature > preTemp && curTimerTs == 0 ){ /**gcs:如果温度上升，并且之前没有设置过定时器，这时候就需要设定一个定时器*/
      //==========================================================f5
      /**gcs:
        * 如果当前的温度大于我们存储在 state 中的温度，这时候我们就需要先获得当前的温度的 timers。
        * 这个 timer 是需要使用 当前的时间 + 5000L 这个偏移量才可以的
        * */
      // 温度上升且没有设过定时器，则注册定时器
      val timerTs = ctx.timerService().currentProcessingTime() + 5000L

      //==========================================================f6
      /**gcs:
        * 之后设定一个 timer 定时器
        * */
      ctx.timerService().registerProcessingTimeTimer( timerTs )

      //==========================================================f7
      /**gcs:
        * 这里为什么要设置当前的 timer 的定时器呢？
        * 因为在接下来，如果我们发现该传感器的温度下降了，我们要删除定时器。因为，在程序运行过程中，我们可能触发了多个定时器，
        * 这时候，当温度下降时，如果我们想注销定时器，我们得拿到上一次注册定时器的时间戳，才可以把定时器给注销掉。
        * 所以，当我们把定时器注册成功之后，我们需要把这个定时器更新到 timers 之中，这样在接下来，如果传感器的温度下降了，我们才可以删除这个定时器。
        * */
      currentTimer.update( timerTs )
    }
  }

  //==========================================================f10
  /**gcs:
    * 设置一个 Timer，指定，当定时器到达时间之后，应该做什么样的操作
    * */
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    //==========================================================f11
    /**gcs:
      * 当温度上升之后，就会输出一条报警信息
      * */
    // 输出报警信息
    out.collect( ctx.getCurrentKey + " 温度连续上升" )
    //==========================================================f12
    /**gcs:
      * 同时清空报警信息
      * */
    currentTimer.clear()
  }
}


class TempChangeAlert(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)]{

  private var lastTempState: ValueState[Double] = _

  /**gcs:
    * 只在这个类创建的时候调用一次。
    * */
  override def open(parameters: Configuration): Unit = {
    // 初始化的时候声明state变量
    lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  }

  /**gcs:
    * 每一个元素都会执行这个 flatmap 函数。
    * 这个 flatMap 函数是将所有的传感器的温度都混合进行处理的，这种方式是不合理的。理论上我们应该对每一种数据都进行一次操作
    * */
  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    //==========================================================f1
    /**gcs:
      * 获取上一次温度
      * */
    // 获取上次的温度值
    val lastTemp = lastTempState.value()
    // 用当前的温度值和上次的求差，如果大于阈值，输出报警信息
    //==========================================================f2
    /**gcs:
      * 如果发现温度升高或者降低了一定阈值
      * */
    val diff = (value.temperature - lastTemp).abs

    //==========================================================f3
    /**gcs:
      * 就会触发报警
      * */
    if(diff > threshold){
      //==========================================================f4
      /**gcs:
        * 往 out 中加入一条记录
        * */
      out.collect( (value.id, lastTemp, value.temperature) )
    }

    //==========================================================f5
    /**gcs:
      * 将我们当前日志的时间更新给 lastTempState。来作为上一个 timesta,p
      * */
    lastTempState.update(value.temperature)
  }

}


/**gcs:
  * keyedProcessFunction
  * 为每一个key提供了一个 valueState 参数
  * */
class TempChangeAlert2(threshold: Double) extends KeyedProcessFunction[String, SensorReading, (String, Double, Double)]{

  //==========================================================f6
  /**gcs:
    * 这是用来存储我们的上一个温度的state的
    * */
  // 定义一个状态变量，保存上次的温度值
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState( new ValueStateDescriptor[Double]("lastTemp", classOf[Double]) )

  //==========================================================f1
  /**gcs:
    * 对每一种key的每一条日志来了之后，都会调用这个 processElement 函数，来处理每一个key下的日志
    * */
  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, (String, Double, Double)]#Context, out: Collector[(String, Double, Double)]): Unit = {

    //==========================================================f2
    /**gcs:
      * 获取上一个温度值
      * */
    // 获取上次的温度值
    val lastTemp = lastTempState.value()

    //==========================================================f3
    /**gcs:
      * 求该key的新数据和上一条数据的时间差
      * */
    // 用当前的温度值和上次的求差，如果大于阈值，输出报警信息
    val diff = (value.temperature - lastTemp).abs
    //==========================================================f4
    /**gcs:
      * 如果该key当前数据和上一条数据的差值到达一定的阈值，这时候就会发送一个报警
      * */
    if(diff > threshold){
      out.collect( (value.id, lastTemp, value.temperature) )
    }
    //==========================================================f5
    /**gcs:
      * 将当前的时间存储到状态存储，来作为上一个时间温度
      * */
    lastTempState.update(value.temperature)
  }
}