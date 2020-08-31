package com.atguigu.wc


import java.util.Properties


import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{ FlinkKafkaConsumer011}
import org.apache.flink.metrics.Gauge


/**gcs:导入这个包，在 addSource 那里才不会报错 */
import org.apache.flink.streaming.api.scala._


case class UserBehavior2( userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)


object metricTest {

    def main(args: Array[String]): Unit = {

        //==========================================================1
        /**gcs:
          * 创建环境
          * */
        // 1. 创建执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        //==========================================================2
        /**gcs:
          * 设定默认的并行度为1
          * */
        env.setParallelism(1)

        //==========================================================3
        /**gcs:
          * 设置 EventTime
          * */
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        //==========================================================4
        /**gcs:
          * 从 kafka 中读取数据
          * */
        // 2. 读取数据
        val properties = new Properties()
        properties.setProperty("bootstrap.servers", "localhost:9092")
        properties.setProperty("group.id", "consumer-group")
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("auto.offset.reset", "latest")

        //==========================================================5
        /**gcs:
          * 从 Kafka 中读取topic
          * */
        //    val sourceStream = env.readTextFile("/Users/congshuo.geng/Documents/david/flink-尚硅谷/3.代码/flink-atGuiGu-all/UserBehaviorAnalysis/HotItemsAnalysis/src/main/resources/UserBehavior.csv")
        val sourceStream = env.addSource( new FlinkKafkaConsumer011[String]("hotItems", new SimpleStringSchema(), properties) ).startNewChain()

        val dataStream = sourceStream.map(new RichMapFunction[String,UserBehavior2] {

            //==========================================================10
            /**gcs:
              * 创建一个我自己的 WaterMarkGauge
              * */
            private var userIdGauge:WaterMarkGauge = new WaterMarkGauge

            override def open(parameters: Configuration): Unit = {

                //==========================================================11
                /**gcs:
                  * 使用 getMetricGroup.gauge[Long,Gauge[Long]]("myWaterMarkGauge",userIdGauge) 来初始化我自己的 guage 类
                  * */
                getRuntimeContext.getMetricGroup.gauge[String,Gauge[String]]("userIdGauge",userIdGauge)
            }

            override def map(value: String):UserBehavior2 ={
                val dataArray = value.split(",")

                //==========================================================12
                /**gcs:
                  * 这里调用 gauge 的 setXXX 方法，channel 中每来一条数据，就会进行一次的set，这样在getValue时才能将value 实时地获得到
                  * */

                userIdGauge.setCurrentUserId(dataArray(0).trim)

                UserBehavior2( dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
            }
        }).startNewChain()

        dataStream.print("dataStream")
        env.execute("metric test")
    }

    //==========================================================9
    /**gcs:
      * 创建一个类，继承 Gauge,注意，合理需要继承 Serializable,否则在 RickFunction 中运行这个类的时候会报错，
      * Gauge 类只有一个函数需要重写，那就是 getValue() 方法。获得我的累加的数
      * */
    class WaterMarkGauge extends Gauge[String] with Serializable {
        @transient private var currentUserId:String = _

        //==========================================================f1
        /**gcs:
          * 我们每次可以调用 getCurrentUserId 方法，每来一条数据就调用一次这个方法
          * */
        def setCurrentUserId(thisCurrentUserId:String)= {
            currentUserId = thisCurrentUserId
        }

        //==========================================================f2
        /**gcs:
          * 这个 getValue 有flink 自己调用，实时地将 value读出来，并且打在Flink的UI的监控中
          * */
        override def getValue: String = {
            currentUserId
        }
    }
}