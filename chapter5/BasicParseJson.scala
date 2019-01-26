// Copyright(C) 2019 All rights reserved.
// @Author: taoye01
// @Created Time: Fri 25 Jan 2019 07:06:20 PM PST
// @Filename: BasicParseJson.scala
// @description:在play框架上，scala读取JSON文件，运行时报错:java.lang.NoClassDefFoundError: play/api/libs/json/OFormat 




import org.apache.spark._
import play.api.libs.json._
import play.api.libs.functional.syntax._

object BasicParseJson {
  case class Person(name: String, lovesPandas: Boolean)
  implicit val personReads = Json.format[Person]

  def main(args: Array[String]) {
    val master = "local"
    val inputFile = "PandaInfo.json"
    val outputFile = "res"
    val sc = new SparkContext(master, "BasicParseJson")
    val input = sc.textFile(inputFile)
    val parsed = input.map(Json.parse(_))
    // We use asOpt combined with flatMap so that if it fails to parse we
    // get back a None and the flatMap essentially skips the result.
    val result = parsed.flatMap(record => personReads.reads(record).asOpt)
    result.filter(_.lovesPandas).map(Json.toJson(_)).saveAsTextFile(outputFile)
    }
}
