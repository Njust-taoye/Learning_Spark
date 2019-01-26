// Copyright(C) 2019 All rights reserved.
// @Author: taoye01
// @Created Time: Fri 25 Jan 2019 07:06:20 PM PST
// @Filename: BasicParseJson.scala
// @description: 第六章关于累加器的代码 

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.DeserializationFeature
import org.apache.spark._
import org.apache.spark.SparkContext._

//import org.eclipse.jetty.client.ContentExchange
import org.eclipse.jetty.client.HttpClient


case class CallLog(callsign: String="", contactlat: Double, contactlong: Double, mylat: Double, mylong: Double)
object BasicAccumlator {
  def main(args: Array[String]) {
    val master = "local"
    val inputFile = "./file/callsigns"
    val outputFile = "res"
    val sc = new SparkContext(master, "BasicAccumlator")
    val file = sc.textFile(inputFile)
    val count = sc.accumulator(0)//累加器，提供了将工作节点中的值聚合到驱动程序中。

    //统计包含字符串"KK6JKQ"
    file.foreach(line => {
      if (line.contains("KK6JKQ")){
        count += 1 
      }
    })
    println("Lines with 'KK6JKQ': " + count.value)
    val errorLines = sc.accumulator(0)
    val dataLines = sc.accumulator(0)
    val validSignCount = sc.accumulator(0)
    val invalidSignCount = sc.accumulator(0)
    val unknownCount = sc.accumulator(0)
    val resolvedCount = sc.accumulator(0)
    //统计空行数
    val callSigns = file.flatMap(line => {
      if (line == ""){
        errorLines += 1
      }else{
        dataLines += 1
      }
      line.split(" ")
    })

    val callSignRegex = "\\A\\d?[a-zA-Z]{1,2}\\d{1,4}[a-zA-Z]{1,3}\\z".r
    //过滤出符合条件的条目
    val validSigns = callSigns.filter{sign => 
     if((callSignRegex findFirstIn sign).nonEmpty){
          validSignCount += 1; true//true保留符合该条件的条目数据
     }else{
          invalidSignCount += 1; false
     }
    }
    val contactCounts = validSigns.map(callSign => (callSign, 1)).reduceByKey((x, y) => x + y)//统计每种条目出现的次数 
    contactCounts.count()
    if(invalidSignCount.value < 0.5 * validSignCount.value){
      contactCounts.saveAsTextFile(outputFile + "/output.txt")
    }else{
      println(s"Too many errors ${invalidSignCount.value} for ${validSignCount.value}")
      System.exit(1)
    }
  }
}
