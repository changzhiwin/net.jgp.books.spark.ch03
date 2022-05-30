package net.jgp.books.spark.ch03.lab320_dataset_books_to_dataframe;

import org.apache.spark.sql.Encoders

import net.jgp.books.spark.basic.Basic
import net.jgp.books.spark.ch03.x.models.Book

object CsvToDatasetBookToDataframe extends Basic{
  def run(): Unit = {
    val spark = getSession("CSV to dataframe to Dataset<Book> and back")

    import spark.implicits._

    val fileName = "./data/books.csv"

    val schema = Encoders.product[Book].schema

    spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

    val ds = spark.read.format("csv").
        schema(schema).
        option("header", "true").
        option("dateFormat", "MM/dd/yy").
        load(fileName).
        as[Book]

    ds.show(5)
    ds.printSchema()

    val booksContainAction = ds.filter(b => b.title.contains("Action")).take(5)

    booksContainAction.foreach(println _)
  }
}