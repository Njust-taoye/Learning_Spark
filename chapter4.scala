// Copyright(C) 2019 All rights reserved.
// @Author: taoye01
// @Created Time: Tue 22 Jan 2019 03:18:22 PM PST
// @Filename: chapter4.scala
// @description: Spark快速大数据分析第四章中scala代码总结
//



val lines = sc.textFile("READMD.md")
val pairs = lines.map(x => (x.split(" ")(0), x)) //map是对每一行做如此操作,使用第一个单词作为键创建一个pair RDD

pairs.filter{case (key, value) => value.length < 20}//对第二个元素进行筛选
pairs.mapValue(x => (x,1)).reduceByKey((x,y) => (x._1+y._1, x._2+y._2))//mapValue会将pair RDD 中的每个value，变成元组(value, 1), 然后再按照key进行归约(reduce)，相同的key的value相加，后面的次数１也相加



/*************实现单词计数*********************/
val input = sc.textFile("README.md")
val words = input.flatMap(x => x.split(" "))//以空格分割出一个一个的单词
val result = words.map(x => (x,1)).reduceByKey((x,y) => x+y)

//上面的reduceByKey等为每个键计算全局的总结果之前会先自动的在每台机子上进行本地合并，用户并不需要指定合并器，更泛华的combineByKey()可以让你自定义合并行为。


/***********使用combineByKey() 求每个键对应的平均值***********************/
val input = sc.parallelize(List(("coffee", 1), ("coffee", 2), ("panda", 3), ("coffee", 9)))
val result = input.combineByKey(
    (v) => (v, 1), // createCombine: 该函数将当前值作为参数，此时我们可以对其做些附加操作, 在这里是将value变成元组
    (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),//mergeValue: 该函数将元素V合并到之前元素C(在各个分区上进行)
    //在不同分区上进行,将不同分区上相同key的value相加
    (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1+acc2._1, acc1._2+acc2._2)
  ).map{ case (key, value) => (key, value._1 /value._2.toFloat)}
result.collectAsMap().map(println(_))//将结果以映射的形式返回，以便查询

/**************自定义reduceByKey() 并行度*******************/
val data = Seq(("a", 3), ("b", 4), ("a", 1))
sc.parallelize(data).reduceByKey((x,y) => x+y) // 默认并行度
sc.parallelize(data).reduceByKey((x,y) => x + y, 10)// 自定义并行度

/*********************连接JOIN**************************/
val rdd1 = sc.parallelize(List((1, "Spark"), (2, "Hadoop"), (3, "Scala"), (4, "Java")))
val rdd2 = sc.parallelize(List((1, "30k"), (2, "15k"), (3, "25k"), (5, "10k")))


val JoinRes = rdd1.join(rdd2).collect.foreach(println)//内连接
//(1,(Spark,30k))
//(2,(Hadoop,15k))
//(3,(Scala,25k))

val LeftJoinRes = rdd1.leftOuterJoin(rdd2).collect.foreach(println)//左外连接
//(4,(Java,None))
//(1,(Spark,Some(30k)))
//(2,(Hadoop,Some(15k)))
//(3,(Scala,Some(25k)))

val RightJoinRes = rdd1.rightOuterJoin(rdd2).collect.foreach(println)//右外连接
//(1,(Some(Spark),30k))
//(5,(None,10k))
//(2,(Some(Hadoop),15k))
//(3,(Some(Scala),25k))

/*******************以字符串对整数进行自定义排序***********************/
val rdd = sc.parallelize(List((1, "Spark"), (2, "Hadoop"), (3, "Scala"), (4, "Java")))
implicit val sortIntegerByString = new Ordering[Int]{
  override def compare(a: Int, b: Int) = a.toString.compare(b.toString)
}
rdd.sortByKey()




