package net.jgp.books.spark;

import net.jgp.books.spark.ch03_lab300_dataset.ArrayToDataset
import net.jgp.books.spark.ch03.lab320_dataset_books_to_dataframe.CsvToDatasetBookToDataframe
import net.jgp.books.spark.ch03.lab200_ingestion_schema_manipulation.IngestionSchemaManipulation

object MainApp {
  def main(args: Array[String]) = {

    // Case: IngestionSchemaManipulation
    IngestionSchemaManipulation.run()

    // Case: CsvToDatasetBookToDataframe
    // CsvToDatasetBookToDataframe.run()

    // Case: ArrayToDataset
    // ArrayToDataset.run()
  }
}