package com.atguigu.apitest

import org.apache.flink.api.common.functions.{FilterFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest
  * Version: 1.0
  *
  * Created by wushengran on 2019/9/17 11:41
  */
object TransformTest {
  def main(args: Array[String]): Unit = {
    //==========================================================1
    /**gcs:
      * 创建环境变量
      * */
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //==========================================================2
    /**gcs:
      * 设置所有的算子的默认的并行度
      * */
    env.setParallelism(1)


    //==========================================================3
    /**gcs:
      * 从文件中读取数据
      * */
    // 读入数据
    val inputStream = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt")


    //==========================================================4
    /**gcs:
      * transform 的操作
      * */
    // Transform操作

    val dataStream = inputStream
      .map(
        data => {
          val dataArray = data.split(",")
          SensorReading( dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble )
        }
      )

    //==========================================================5
    /**gcs:
      * 先按照 SensorReading中的0个元素进行 keyBy操作，之后再在分组的基础之上，对SensorReading中的第1个元素进行求sum的操作
      * */
    dataStream.keyBy(0).sum(1)


    //==========================================================5
    /**gcs:
      * 聚合
      * */
    // 1. 聚合操作
     val stream1 = dataStream
       .keyBy(_.id) //keyBy(0)
//      .sum("temperature")
        //==========================================================6
      .reduce( (x, y) => SensorReading(x.id, x.timestamp + 1, y.temperature + 10) )

    //==========================================================7
    /**gcs:
      * 使用 split 算子对 DataStream 进行分流
      * */
    // 2. 分流，根据温度是否大于30度划分
    val splitStream = dataStream
      .split( sensorData => {
        //==========================================================8
        /**gcs:
          * 根据温度是否大于30，将 DataStream 分成两个 high 和 low 的流
          * */
        if( sensorData.temperature > 30 ) Seq("high") else Seq("low")
      } )

    //==========================================================9
    /**gcs:
      * 将名字为 high 的分流 提取出来
      * */
    val highTempStream = splitStream.select("high")

    /**gcs:high 子流输出*/

    //==========================================================10
    /**gcs:将名字为 low 的分流提取出来*/
    val lowTempStream = splitStream.select("low")

    /**gcs:low 子流输出*/

    //==========================================================11
    /**gcs:
      * 将名字为 high low的两个分流提取出来。这意味着这个操作可以提取两个流
      * */
    val allTempStream = splitStream.select("high", "low")


    //==========================================================12
    /**gcs:
      * 使用 connect 算子来合并两个流
      * */
    // 3. 合并两条流
    val warningStream = highTempStream.map( sensorData => (sensorData.id, sensorData.temperature) )

    //==========================================================13
    /**gcs:
      * 将 warningStream 和 lowTempStream 使用 connect 算子进行合并
      * */
    val connectedStreams = warningStream.connect(lowTempStream)

    //==========================================================14
    /**gcs:
      * 使用 CoMap 和 CoFlatMap 方法。
      * 使用 warningData => ( warningData._1, warningData._2, "high temperature warning" ) 方法来对上面的 warningStream 进行操作。
      * 之后使用 lowData => ( lowData.id, "healthy" ) 来对 lowStream 进行操作
      * */
    val coMapStream = connectedStreams.map(
      warningData => ( warningData._1, warningData._2, "high temperature warning" ),
      lowData => ( lowData.id, "healthy" )
    )

    //==========================================================15
    /**gcs:
      * def union(dataStreams: DataStream[T]*): DataStream[T]
      * 我们可以看到 union 算子可以同时将多个 DataStream 进行 union 的操作
      * */
    val unionStream = highTempStream.union(lowTempStream)

    // 函数类
    dataStream.filter( new MyFilter() ).print()
    env.execute("transform test job")
  }
}

class MyFilter() extends FilterFunction[SensorReading]{
  override def filter(value: SensorReading): Boolean = {
    value.id.startsWith("sensor_1")
  }
}



class MyMapper() extends RichMapFunction[SensorReading, String]{
  override def map(value: SensorReading): String = {
    "flink"
  }

  override def open(parameters: Configuration): Unit = super.open(parameters)
}