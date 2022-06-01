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
> you should change the entry in the MainApp.scala
### Case: IngestionSchemaManipulation
```
root
 |-- datasetId: string (nullable = true)
 |-- name: string (nullable = true)
 |-- address1: string (nullable = true)
 |-- address2: string (nullable = true)
 |-- city: string (nullable = true)
 |-- state: string (nullable = true)
 |-- zip: string (nullable = true)
 |-- tel: string (nullable = true)
 |-- dateStart: string (nullable = true)
 |-- type: string (nullable = true)
 |-- geoX: string (nullable = true)
 |-- geoY: string (nullable = true)
 |-- county: string (nullable = false)
 |-- id: string (nullable = true)


==== Wake 3440, Durham 2463, All 5903
==== Partition( 1, 1, 2)
```

### Case: CsvToDatasetBookToDataframe
```
+---+--------+--------------------+-----------+--------------------+
| id|authorId|               title|releaseDate|                link|
+---+--------+--------------------+-----------+--------------------+
|  1|       1|Fantastic Beasts ...| 2016-11-18|http://amzn.to/2k...|
|  2|       1|Harry Potter and ...| 2015-10-06|http://amzn.to/2l...|
|  3|       1|The Tales of Beed...| 2008-12-04|http://amzn.to/2k...|
|  4|       1|Harry Potter and ...| 2016-10-04|http://amzn.to/2k...|
|  5|       2|Informix 12.10 on...| 2017-04-23|http://amzn.to/2i...|
+---+--------+--------------------+-----------+--------------------+
only showing top 5 rows


root
 |-- id: integer (nullable = true)
 |-- authorId: integer (nullable = true)
 |-- title: string (nullable = true)
 |-- releaseDate: date (nullable = true)
 |-- link: string (nullable = true)


Book(13,Some(6),Spring Boot in Action,Some(2016-01-03),http://amzn.to/2hCPktW)
Book(14,Some(6),Spring in Action: Covers Spring 4,Some(2014-11-28),http://amzn.to/2yJLyCk)
Book(17,Some(9),Java 8 in Action: Lambdas, Streams, and functional-style programming,Some(2014-08-28),http://amzn.to/2isdqoL)
```

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
Becase the data in the ./data/books.csv is very bad format.
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
