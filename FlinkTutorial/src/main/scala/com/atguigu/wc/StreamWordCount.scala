package com.atguigu.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {

    //==========================================================1
    /**gcs:
      * 接收  host和port，这样一会儿启动起来我们的程序之后，
      * 就可以往这个host:port 中发送信息了
      * */
    val params = ParameterTool.fromArgs(args)
    val host: String = params.get("host")
    val port: Int = params.getInt("port")

    //==========================================================2
    /**gcs:
      * 创建一个 streaming 的环境
      * */
    // 创建一个流处理的执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)
    //==========================================================3
    /**gcs:
      * 从 host:port 中接收数据
      * */
    // 接收socket数据流
    val textDataStream = env.socketTextStream(host, port)

    //==========================================================4
    /**gcs:
      * wordCount 的计算
      * */
    // 逐一读取数据，分词之后进行wordcount
    val wordCountDataStream = textDataStream.flatMap(_.split("\\s")).disableChaining()
      .filter(_.nonEmpty)
      .map( (_, 1) ).setParallelism(2)
      .keyBy(0).sum(1).setParallelism(2)

    //==========================================================5
    /**gcs:
      * 输出
      * */
    // 打印输出
    wordCountDataStream

    //==========================================================6
    /**gcs:
      * 上面定义好了我们的程序的执行流程，但是我们的程序并没有真正的运行，
      * 只有当我们运行了 execute() 这个函数之后，我们的Flink程序才会真正的开始运行
      * */
    // 执行任务
    env.execute("stream word count job")
  }
}
