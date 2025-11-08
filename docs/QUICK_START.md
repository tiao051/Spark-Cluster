# Quick Start Guide

## 1. Start the Cluster

```powershell
docker-compose up -d
```

## 2. Run a Python Script

```powershell
docker exec spark-master /spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --driver-java-options "-Dlog4j.configuration=file:/workspace/config/log4j.properties" `
  --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/workspace/config/log4j.properties" `
  --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:/workspace/config/log4j.properties" `
  /workspace/scripts/python/basic_rdd.py
```

## 3. Run Scala Code in REPL

```powershell
docker exec -it spark-master /spark/bin/spark-shell --master spark://spark-master:7077
```

Then in the REPL:

```scala
:load /workspace/scripts/scala/rdd_operations.scala
```

## 4. Stop the Cluster

```powershell
docker-compose down
```
