// Copyright(C) 2019 All rights reserved.
// @Author: taoye01
// @Created Time: Sat 26 Jan 2019 11:05:41 AM PST
// @Filename: BasicParseJsonWithJackson.scala
// @description: 


import org.apache.spark._
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.DeserializationFeature


case class Person(name: String, lovesPandas: Boolean)

object BasicParseJsonWithJackson{
  def main(args: Array[String]){
    val master = "local"
    val inputFile = "PandaInfo.json"
    val outputFile = "withJson"
    val sc = new SparkContext(master, "BasicParseJsonWithJackson")
    val input = sc.textFile(inputFile)
    //mapPartitions:避免对每个元素进行重复的配置工作，而是只RDD的每个分区进行一次配置（基于分区）

    val result = input.mapPartitions(records => {
      val mapper = new ObjectMapper with ScalaObjectMapper
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      mapper.registerModule(DefaultScalaModule)

      //将其解析为特定的case class。使用flatMap,通过在遇到问题时返回空列表来处理错误。在没有问题时返回一个包含一个元素的列表(Some(_))
      records.flatMap(record => {
        try{
          Some(mapper.readValue(record, classOf[Person]))
        }catch{
          case e: Exception => None
        }
      })
    }, true)
    //过滤出结果中lovesPandas为真的条目,再利用基于分区的mapPartitions避免重复操作。
    result.filter(_.lovesPandas).mapPartitions(records => {
      val mapper = new ObjectMapper with ScalaObjectMapper
      mapper.registerModule(DefaultScalaModule)
      records.map(mapper.writeValueAsString(_))
    }).saveAsTextFile(outputFile)
  }
}
