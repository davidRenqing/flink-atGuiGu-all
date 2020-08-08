package com.atguigu.apitest.sinktest

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.atguigu.apitest.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest.sinktest
  * Version: 1.0
  *
  * Created by wushengran on 2019/9/17 16:44
  */
object JdbcSinkTest {
  def main(args: Array[String]): Unit = {
    //==========================================================1
    /**gcs:
      * 获得执行环境
      * */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //==========================================================2
    /**gcs:
      * 读取数据
      * */
    // source
    val inputStream = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    //==========================================================3
    /**gcs:
      * 数据解析
      * */
    // transform
    val dataStream = inputStream
      .map(
        data => {
          val dataArray = data.split(",")
          SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
        }
      )

    //==========================================================4
    /**gcs:
      *
      * */
    // sink
    dataStream.addSink( new MyJdbcSink() )

    env.execute("jdbc sink test")
  }
}

class MyJdbcSink() extends RichSinkFunction[SensorReading]{
  // 定义sql连接、预编译器
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  // 初始化，创建连接和预编译语句
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456")
    insertStmt = conn.prepareStatement("INSERT INTO temperatures (sensor, temp) VALUES (?,?)")
    updateStmt = conn.prepareStatement("UPDATE temperatures SET temp = ? WHERE sensor = ?")
  }

  // 调用连接，执行sql
  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    // 执行更新语句
    updateStmt.setDouble(1, value.temperature)
    updateStmt.setString(2, value.id)
    updateStmt.execute()
    // 如果update没有查到数据，那么执行插入语句
    if( updateStmt.getUpdateCount == 0 ){
      insertStmt.setString(1, value.id)
      insertStmt.setDouble(2, value.temperature)
      insertStmt.execute()
    }
  }

  // 关闭时做清理工作
  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}