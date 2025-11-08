sc.setLogLevel("ERROR")

val listRdd = sc.parallelize(List(1, 2, 3, 4, 5))
val inputRDD = sc.parallelize(List(("A", 20), ("B", 30), ("C", 40), ("Z", 1)))

// fold
println("fold : " + listRdd.fold(0){ (acc, v) => acc + v })
println("fold : " + inputRDD.fold(("Total", 0)){ (acc, v) => ("Total", acc._2 + v._2) })

// min
println("min : " + listRdd.min())
println("min : " + inputRDD.min())

// max
println("max : " + listRdd.max())
println("max : " + inputRDD.max())