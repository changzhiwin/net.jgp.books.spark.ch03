package net.jgp.books.spark.ch03.x.models

import java.sql.Date
//import java.sql.Timestamp

case class Book(id: Int, 
    authorId: Option[Int], 
    title: String, 
    releaseDate: Option[Date], 
    link: String
)