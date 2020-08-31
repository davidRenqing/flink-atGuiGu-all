package com.atguigu.networkflow_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.networkflow_analysis
  * Version: 1.0
  *
  * Created by wushengran on 2019/9/23 11:34
  */
object UvWithBloom {
  def main(args: Array[String]): Unit = {
    //==========================================================1
    /**gcs:
      * 创建环境
      * */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 用相对路径定义数据源
    val resource = getClass.getResource("/UserBehavior.csv")
    val dataStream = env.readTextFile(resource.getPath)
      .map(data => {
        //==========================================================2
        /**gcs:
          * 数据规整化处理
          * */
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
      })
        //==========================================================3
        /**gcs:
          * 指定 UserBehavior 中的 timestamp 作为 eventTime
          * */
      .assignAscendingTimestamps(_.timestamp * 1000L)
        //==========================================================4
        /**gcs:
          * 进行数据 filter 的操作
          * */
      .filter(_.behavior == "pv") // 只统计pv操作
      .map(data => ("dummyKey", data.userId))
      .keyBy(_._1)

      .timeWindow(Time.hours(1))
        //==========================================================5
      .trigger(new MyTrigger())
        //==========================================================6
      .process(new UvCountWithBloom())

    dataStream.print()

    env.execute("uv with bloom job")
  }
}

//==========================================================5.1
/**gcs:
  * 自定义触发器。触发器主要是指定什么时间来调用timeWindow() 函数后面的 process 函数的操作的。
  * 如果我们不指定 trigger() 函数，默认情况下是等到该window窗口结束之后，才会触发 process 函数的操作。
  * 如果我们指定 trigger() 函数，我们就可以自定义地指定在该window窗口没有结束之前，的中间的什么时间时候来触发 trigger 后面的 process 函数的操作。
  * e.g. 遇到什么元素触发window窗口操作? 时间时间触发窗口操作？
  * 以及，我们可以指定在触发 process 函数的操作之后，元素应该怎么操作。
  * 我们可以指定在触发 window 操作之后，窗口的元素仍然被留下，我们也可以指定在触发触发窗口操作之后，将该窗口中的所有的元素都清空
  * FIRE 就是触发 timeWindow() 后面的 process() 函数的操作。
  * PURGE 就是指清空窗口的操作。
  * FIRE_AND_PURGE 就是指触发窗口执行trigger后面 process()，并且清空当前窗口
  * 在这个例子中，我们只是实现了 onElement 函数，指定每来一条元素就触发窗口的操作，执行窗口后面的 process() 函数来处理元素。
  * 在将元素处理完成之后，就将该window窗口中的所有的元素都给清空
  * */
// 自定义窗口触发器
class MyTrigger() extends Trigger[(String, Long), TimeWindow] {
  //==========================================================f1
  /**gcs:
    * 指定按照 eventTime 的方式来对窗口进行触发。
    * time 是当前日志的时间。window 是当前的window。我们可以根据 time 和 window 的比例来对比什么时候对数据进行触发
    * */
  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  //==========================================================f2
  /**gcs:
    * 指定按照 process time 的方式来对窗口进行触发。
    * */
  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}

  //==========================================================f3
  /**gcs:
    * 指定对每一条元素触发一次操作。对每来一条元素，就执行 6 中的 process() 函数中的逻辑，进行 bloom 过滤器的操作，来对这一条数据进行处理。对这条数据处理完成之后，就清空window窗口，删除这条数据
    * */
  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    //==========================================================f4
    /**gcs:
      * 在这个触发器中，我们
      * */
    // 每来一条数据，就直接触发窗口操作，并清空所有窗口状态
    TriggerResult.FIRE_AND_PURGE
  }
}


//==========================================================6.1
/**gcs:
  * 定义一个 布隆过滤器，并且设置 size。比如，我们设置 size=32，那么我们就可以 2的32次幂 是 0.5 GB 。
  * 所以当 size= 1<<32 时，代表我们有一个 1<<32 => 2的32次幂 大小的数组，这个数组中的每一位都可以用来判断一个userId 存不存在。我们有 0.5GB 的内存可以存储布隆过滤器的hash值
  * */
// 定义一个布隆过滤器
class Bloom(size: Long) extends Serializable {
  //==========================================================f1
  /**gcs:
    * 判断位图的大小，如果没有指定布隆过滤器的大小，就默认使用 size = 27 位，即 布隆过滤器占有
    * */
  // 位图的总大小，默认16M = 1024*16 KB = 1024*16*1024 B = 1024*16*1024*8 Bytes = 2的 10+4+10+3 =2的27次幂
  private val cap = if (size > 0) size else 1 << 27

  //==========================================================f2
  /**gcs:
    * 定义一个 hash函数，将每一个 string类型的 userId 对应到一个 index 值中
    * */
  // 定义hash函数
  def hash(value: String, seed: Int): Long = {
    /**gcs:
      * 得到的最终的结果，是一个 Long 类型的值。代表我这个 userId 被映射成为了一个Long 类型的 index 下坐标
      * */

    //==========================================================f3
    /**gcs:
      * 定义hash的映射方法。
      * 遍历 value 中的每一位，之后让 result * seed 再加上 value
      * */
    var result = 0L
    for( i <- 0 until value.length ){
      result = result * seed + value.charAt(i)
    }

    //==========================================================f4
    /**gcs:
      * 将 result 与 位图的大小进行 "与" 运算。因为我们只要取result 中的前 cap 位，就把该result 映射到 0~2的 cap 次幂 之间
      * */
    result  & ( cap - 1 )
  }
}

//==========================================================6.1
/**gcs:
  *
  * 定义一个*/
class UvCountWithBloom() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow]{

  //==========================================================f1
  /**gcs:
    * 创建 redis 的连接
    * */
  // 定义redis连接
  lazy val jedis = new Jedis("localhost", 6379)

  //==========================================================f2
  /**gcs:
    * 定义了一个 1<<29 大小的布隆过滤器，大小是 64M，可以存储5亿多条记录
    * */
  lazy val bloom = new Bloom(1<<29)

  //==========================================================f3
  /**gcs:
    * 定义 process 函数，每次 window 窗口被触发的时候就会执行这个 process 函数
    * */
  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {

    //==========================================================f3.1
    /**gcs:
      * 设定 redis 的key
      * 因为我们每一个窗口都有一个布隆过滤器，所以我们使用 window 窗口的结束时间的时间戳 context.window.getEnd.toString 作为 redis 的key，存储该窗口中当前的 userId 的统计数量
      * 老师这里面将每一个窗口的key用在了 redis中存储，而实际上，我们可以优化成将这个 userId 作为一个 state 存储在程序里
      * 我们使用 redis 存储了两部分的值。一，该窗口当前的userId 的统计量。每一个window的值，用<storeKey,count> 的方式存储在了一个redis的Map当中
      * 二，该窗口中的布隆过滤器，使用 getBit 方法，按照 storeKey，byte[] 数组的方法存储在了 redis中，可以使用
      * */
    // 位图的存储方式，key是windowEnd，value是bitmap
    val storeKey = context.window.getEnd.toString
    var count = 0L
    //==========================================================f3.2
    /**gcs:
      * 如果发现这个窗口中的 window.end.toString 不为空，这时候说明当前这个 window窗口是有值的
      * */
    // 把每个窗口的uv count值也存入名为count的redis表，存放内容为（windowEnd -> uvCount），所以要先从redis中读取
    if( jedis.hget("count", storeKey) != null ){
      //==========================================================f3.3
      /**gcs:
        * 获得当前的 window.eng.toString 的 userId 的值
        * */
      count = jedis.hget("count", storeKey).toLong
    }
    //==========================================================f3.4
    // 用布隆过滤器判断当前用户是否已经存在
    val userId = elements.last._2.toString
    //==========================================================f3.5
    /**gcs:
      * 首先先将 userId 使用 hash 函数映射成为一个布隆过滤器中的byte[] 数组中的一个下坐标值
      * */
    val offset = bloom.hash(userId, 61)

    //==========================================================f3.6
    /**gcs:
      * 使用 gitbit 方法，从存储在redis中的布隆过滤器中取出 offset那一位的值。
      * 如果这个值为1，说明该 userId 之前可能出现过。如果这个值为0，说明该 userId 之前一定没有出现过
      * */
    // 定义一个标识位，判断reids位图中有没有这一位
    val isExist = jedis.getbit(storeKey, offset)
    //==========================================================f3.7
    if(!isExist){
      /**gcs:
        * 如果这个 userId 在该布隆过滤器中没有出现过，这时候就会首先使用 setbit 的方法，将该userId 对应的位置设置为1
        * */
      // 如果不存在，位图对应位置1，count + 1
      jedis.setbit(storeKey, offset, true)
      //==========================================================f3.8
      /**gcs:
        * 并且累加 storeKey 中的值1
        * */
      jedis.hset("count", storeKey, (count + 1).toString)

      //==========================================================f3.9
      /**gcs:
        * 将该window当前的 userId+1 再输出出去
        * */
      out.collect( UvCount(storeKey.toLong, count + 1) )
    } else {
      //==========================================================f3.10
      /**gcs:
        * 否则 该 window窗口 userId 的值不变，输出出去
        * */
      out.collect( UvCount(storeKey.toLong, count) )
    }
  }
}