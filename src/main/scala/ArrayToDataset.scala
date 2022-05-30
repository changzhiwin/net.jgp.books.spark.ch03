package net.jgp.books.spark.ch03_lab300_dataset;

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession

import net.jgp.books.spark.basic.Basic

object ArrayToDataset extends Basic {

  def run(): Unit = {
    val spark = getSession("String Array to Dataset<String>")

    val stringSeq = Seq("Jean", "Liz", "Pierre", "Lauric")
    
    import spark.implicits._

    // An implicit Encoder[String] is needed
    val ds = spark.createDataset(stringSeq)

    ds.show()

    ds.printSchema()
  }

}