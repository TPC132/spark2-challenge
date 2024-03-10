package com.challenge

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * @author ${user.name}
 */
object App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark 2 Application")
      .master("local")
      .getOrCreate()


    //---------PART1---------
    val df_1: DataFrame = spark.read
      .option("header", value = true)
      .option("delimiter", ",")
      .csv("resources/googleplaystore_user_reviews.csv")
      .select("App", "Sentiment_Polarity")
      .withColumn("Sentiment_Polarity", col("Sentiment_Polarity").cast("double"))
      .na.fill(0, Seq("Sentiment_Polarity"))
      .groupBy("App")
      .agg(avg("Sentiment_Polarity").as("Average_Sentiment_Polarity"))


    //---------PART2---------
    val df_2: DataFrame = spark.read
      .option("header", value = true)
      .option("delimiter", ",")
      .csv("resources/googleplaystore.csv")
      .withColumn("Rating", col("Rating").cast("double"))
      .filter(col("Rating") >= 4.0)
      .orderBy(col("Rating").desc)

    /*  df_2.write
        .option("header", "true")
        .option("delimiter", "§")
        .csv("results/best_apps.csv")*/

    //---------PART3---------
    val gpsDf: DataFrame = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .csv("resources/googleplaystore.csv")

    //df with removed app duplicates, string of categories and highest number of reviews
    val optDF: DataFrame = gpsDf
      .withColumn(("Reviews"), col("Reviews").cast("long"))
      .groupBy("App")
      .agg(
        collect_set("Category").as("Categories"),
        max("Reviews").as("Reviews")
      )

    val joinDF: DataFrame = optDF
      .join(gpsDf, Seq("App", "Reviews"))
      .drop("Category")
      .distinct()


    val df_3: DataFrame = joinDF
      .selectExpr("App",
        "Categories",
        "cast(Rating as double) Rating",
        "Reviews",
        "Size",
        "Installs",
        "Type",
        "Price",
        "`Content Rating` AS Content_Rating",
        "Genres",
        "`Last Updated` AS Last_Updated",
        "`Current Ver` AS Current_Version",
        "`Android Ver` AS Minimum_Android_Version")
      .na.fill(0, Seq("Reviews"))
      .withColumn("Size", regexp_replace(col("Size"), "M", "").cast("double"))
      .withColumn("Price", regexp_replace(col("Price"), "\\$", "").cast("double"))
      .withColumn("Price", round(col("Price") * 0.9, 2))
      .withColumn("Genres", split(col("Genres"), ";"))
      .withColumn("Last_Updated", date_format(to_date(col("Last_Updated"), "MMMM d, yyyy"), "yyyy-MM-dd").cast("date"))

    val teste = df_3.select("App").count()

    println(teste)


    //---------PART4---------
    val df_1_3: DataFrame = df_3
      .join(df_1, Seq("App"), "left_outer")
      .select("App",
        "Categories",
        "Rating",
        "Reviews",
        "Size",
        "Installs",
        "Type",
        "Price",
        "Content_Rating",
        "Genres",
        "Last_Updated",
        "Current_Version",
        "Minimum_Android_Version",
        "Average_Sentiment_Polarity"
      )

    df_1_3.write
      .format("parquet")
      .option("compression", "gzip")
      .save("googleplaystore_cleaned")


    //---------PART5---------
    val df_4: DataFrame = df_3
      .join(df_1, Seq("App"), "left_outer")
      .select("App",
        "Rating",
        "Genres",
        "Average_Sentiment_Polarity"
      )
      .withColumn("Genres", explode(col("Genres")))
      .groupBy("Genres")
      .agg(
        count("App").as("Count"),
        avg("Rating").as("Average_Rating"),
        avg("Average_Sentiment_Polarity").as("Average_Sentiment_Polarity")
      )

    df_4.write
      .format("parquet")
      .option("compression", "gzip")
      .save("googleplaystore_metrics")

    df_4.show(100)

    spark.stop()
  }


}