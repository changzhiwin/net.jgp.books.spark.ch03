package net.jgp.books.spark.ch03.lab200_ingestion_schema_manipulation;

import org.apache.spark.sql.functions.{lit, concat, split}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

import net.jgp.books.spark.basic.Basic

object IngestionSchemaManipulation extends Basic{
  def run(): Unit = {
    val spark = getSession("Restaurants in Wake County, NC")

    val wakeDFWithID = ingestFromCSV(spark)
    val durhamDFWithID = ingestFromJson(spark)
    val all = combineWakeAndDurham(wakeDFWithID, durhamDFWithID)

    println(s"==== Wake ${wakeDFWithID.count}, Durham ${durhamDFWithID.count}, All ${all.count}")
    println(s"==== Partition( ${wakeDFWithID.rdd.partitions.length}, ${durhamDFWithID.rdd.partitions.length}, ${all.rdd.partitions.length})")
  }

  def combineWakeAndDurham(wake: DataFrame, durham: DataFrame): DataFrame = {
    wake.unionByName(durham)
  }

  def ingestFromCSV(spark: SparkSession): DataFrame = {
    val df = spark.read.
      format("csv").
      option("header", "true").
      load("data/Restaurants_in_Wake_County_NC.csv")
    // df.printSchema

    val wakeDF = df.withColumn("county", lit("Wake")).
      withColumnRenamed("HSISID", "datasetId").
      withColumnRenamed("NAME", "name").
      withColumnRenamed("ADDRESS1", "address1").
      withColumnRenamed("ADDRESS2", "address2").
      withColumnRenamed("CITY", "city").
      withColumnRenamed("STATE", "state").
      withColumnRenamed("POSTALCODE", "zip").
      withColumnRenamed("PHONENUMBER", "tel").
      withColumnRenamed("RESTAURANTOPENDATE", "dateStart").
      withColumnRenamed("FACILITYTYPE", "type").
      withColumnRenamed("X", "geoX").
      withColumnRenamed("Y", "geoY").
      drop("OBJECTID").
      drop("PERMITID").
      drop("GEOCODESTATUS")

    import spark.implicits._
    val wakeDFWithID = wakeDF.withColumn("id", concat($"state", lit("_"), $"county", lit("_"), $"datasetId"))
    wakeDFWithID.printSchema

    wakeDFWithID
  }

  def ingestFromJson(spark: SparkSession): DataFrame = {
    val df = spark.read.
      format("json").
      load("data/Restaurants_in_Durham_County_NC.json")

    import spark.implicits._
    val durhamDF = df.withColumn("county", lit("Durham")).
      withColumn("datasetId", $"fields.id").
      withColumn("name", $"fields.premise_name").
      withColumn("address1", $"fields.premise_address1").
      withColumn("address2", $"fields.premise_address2").
      withColumn("city", $"fields.premise_city").
      withColumn("state", $"fields.premise_state").
      withColumn("zip", $"fields.premise_zip").
      withColumn("tel", $"fields.premise_phone").
      withColumn("dateStart", $"fields.opening_date").
      //withColumn("dateEnd", $"fields.closing_date").
      withColumn("type", split($"fields.type_description", " - ").getItem(1)).
      withColumn("geoX", ($"fields.geolocation").getItem(0)).
      withColumn("geoY", ($"fields.geolocation").getItem(1)).
      // !!!Notice: Cloumn's name is case insensitive, datasetId and datasetid
      drop("fields").
      drop("geometry").
      drop("recordid").
      drop("record_timestamp")

    val durhamDFWithID = durhamDF.withColumn("id", concat($"state", lit("_"), $"county", lit("_"), $"datasetId"))
    durhamDFWithID.printSchema

    durhamDFWithID
  }
}