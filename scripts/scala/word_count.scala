val rdd = sc.parallelize(
List("Germany India USA","USA India Russia","India Brazil Canada China")
)
val wordsRdd = rdd.flatMap(_.split(" "))
val pairRDD = wordsRdd.map(f=>(f,1))

println("=== Word Pairs ===")
pairRDD.collect().foreach(println)

println("\n=== Word Count ===")
val wordCount = pairRDD.reduceByKey(_ + _)
wordCount.collect().foreach(x => println(s"${x._1}: ${x._2}"))

println("\n=== Total Unique Words ===")
println(wordCount.count())
