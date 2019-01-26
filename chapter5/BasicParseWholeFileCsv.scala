// Copyright(C) 2019 All rights reserved.
// @Author: taoye01
// @Created Time: Sat 26 Jan 2019 12:08:38 PM PST
// @Filename: BasicParseCsv.scala
// @description:example5-13、5-19 


import java.io.StringReader
import java.io.StringWriter

import org.apache.spark._
import play.api.libs.json._
import play.api.libs.functional.syntax._
import scala.util.parsing.json.JSON
import scala.collection.JavaConversions._

import au.com.bytecode.opencsv.CSVReader
import au.com.bytecode.opencsv.CSVWriter


object BasicParseWholeFileCsv{

  def main(args: Array[String]){
    val master = "local"
    val inputFile = "inputFile.csv"
    val outputFile = "res"
    val sc = new SparkContext(master, "BasicParseWholeFileCsv")
    val input = sc.wholeTextFiles(inputFile)//当文件足够小时，可以使用SparkContext.wholeTextFiles()读取整个文件,
   //该方法会返回一个pair RDD,其中键为输入文件名。
    val result = input.flatMap{ case (_, txt) => 
        val reader = new CSVReader(new StringReader(txt));
        reader.readAll()
    }
  println(result.collect().map(_.toList).mkString(","))
  }
}
      
