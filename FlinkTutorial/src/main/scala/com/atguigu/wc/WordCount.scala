package com.atguigu.wc

import org.apache.flink.api.scala._

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.wc
  * Version: 1.0
  *
  * Created by wushengran on 2019/9/16 11:48
  */

// 批处理代码
object WordCount {
  def main(args: Array[String]): Unit = {

    //==========================================================1
    // 创建一个批处理的执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    //==========================================================2
    /**gcs:
      * 从文件中读取数据
      * */
    // 从文件中读取数据
    val inputPath = "/Users/congshuo.geng/Documents/david/flink-尚硅谷/3.代码/FlinkTutorial/src/main/resources/hello.txt"
    val inputDataSet = env.readTextFile(inputPath)

    //==========================================================3
    /**gcs:
      * 之后将我们的日志按照空格切分，打散。
      * 然后，按照第0个元素进行 groupBy 的操作，
      * 然后做 sum 的操作
      * */
    // 分词之后做count
    val wordCountDataSet = inputDataSet.flatMap(_.split(" "))
      .map( (_, 1) )
      .groupBy(0)
      .sum(1)

    // 打印输出
    wordCountDataSet.print()
  }
}

// /Users/congshuo.geng/Documents/david/flink-尚硅谷/3.代码/FlinkTutorial/src/main/resources/hello.txt
// /Users/congshuo.geng/Documents/david/flink-尚硅谷/3.代码/FlinkTutorial/src/main/scala/com/atguigu/wc/WordCount.scala