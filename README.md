# Purpose
pure scala version of https://github.com/jgperrin/net.jgp.books.spark.ch03

# Environment
- Java 11
- Scala 2.13.8
- Spark 3.2.1

# How to run
## 1, sbt package, in project root dir
When success, there a jar file at ./target/scala-2.13. The name is `main-scala-ch3_2.13-1.0.jar` (the same as name property in sbt file)

## 2, submit jar file, in project root dir
```
$ YOUR_SPARK_HOME/bin/spark-submit \
  --class net.jgp.books.spark.MainApp \
  target/scala-2.13/main-scala-ch3_2.13-1.0.jar
```

## 3, print

### Case: string to Dataset
```
+------+
| value|
+------+
|  Jean|
|   Liz|
|Pierre|
|Lauric|
+------+
```

## 4, Some diffcult case

### Date format 
```
// 3.0 changed
spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

// for timestamp
option("timestampFormat", "MM/dd/yyyy HH:mm")

// for date
option("dateFormat", "MM/dd/yyyy")
```

### Dateset's type can not have null value, use Option
```
case class Book(id: Int, 
    authorId: Option[Int], 
    title: String, 
    releaseDate: Option[Date], 
    link: String
)
```
