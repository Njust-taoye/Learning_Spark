// Copyright(C) 2019 All rights reserved.
// @Author: taoye01
// @Created Time: Sat 26 Jan 2019 12:08:38 PM PST
// @Filename: BasicParseCsv.scala
// @description: 


import java.io.StringReader
import java.io.StringWriter

import org.apache.spark._
import play.api.libs.json._
import play.api.libs.functional.syntax._
import scala.util.parsing.json.JSON
import scala.collection.JavaConversions._

import au.com.bytecode.opencsv.CSVReader
import au.com.bytecode.opencsv.CSVWriter


object BasicParseCsv{
  case class Person(name: String, favouriteAnimal: String)

  def main(args: Array[String]){
    val master = "local"
    val inputFile = "inputFile.csv"
    val outputFile = "res"
    val sc = new SparkContext(master, "BasicParseCsv")
    val input = sc.textFile(inputFile)
    //一行一行的读取csv文件
    val result = input.map{ line => 
      val reader = new CSVReader(new StringReader(line));
      reader.readNext();
    }
  
    val people = result.map(x => Person(x(0), x(1)))//解析为Person类
    val pandaLovers = people.filter(person => person.favouriteAnimal == "panda")//过滤出favouriteAnimal为panda的条目
    //写csv文件
    pandaLovers.map(person => List(person.name, person.favouriteAnimal).toArray).mapPartitions{people => 
      val stringWriter = new StringWriter();
      val csvWriter = new CSVWriter(stringWriter);
      csvWriter.writeAll(people.toList)
      Iterator(stringWriter.toString)
    }.saveAsTextFile(outputFile)
  }
}
      
