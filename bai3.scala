val rdd = sc.parallelize(
List("Germany India USA","USA India Russia","India Brazil Canada China")
)
val wordsRdd = rdd.flatMap(_.split(" "))
val pairRDD = wordsRdd.map(f=>(f,1))
pairRDD.foreach(println)