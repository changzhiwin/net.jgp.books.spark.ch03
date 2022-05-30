package net.jgp.books.spark.ch03.lab200_ingestion_schema_manipulation;

import org.apache.spark.sql.functions.{lit, concat}
import net.jgp.books.spark.basic.Basic

object IngestionSchemaManipulation extends Basic{
  def run(): Unit = {
    val spark = getSession("Restaurants in Wake County, NC")

    val df = spark.read.
      format("csv").
      option("header", "true").
      load("data/Restaurants_in_Wake_County_NC.csv")

    // some print
    df.printSchema

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
    wakeDFWithID.show(5)
  }
}