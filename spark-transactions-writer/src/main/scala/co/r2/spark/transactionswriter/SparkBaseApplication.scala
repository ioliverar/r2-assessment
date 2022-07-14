package co.r2.spark.transactionswriter

import java.text.SimpleDateFormat
import java.util.Calendar

import co.r2.spark.transactionswriter.storage.DAOStorage._
import co.r2.spark.transactionswriter.commons.Util
import org.apache.spark.sql.functions._

object SparkBaseApplication extends App with SparkJob{

  //Setting date and hour format
  val sdf = new SimpleDateFormat(Util.DATE_PROCESS_FORMAT)
  val processDate = sdf.format(Calendar.getInstance.getTime)
  val sd = new SimpleDateFormat(Util.DATE_FILE)
  val batchDate = "20200119" //sd.format(Calendar.getInstance.getTime)

  //Defining Paths
  val srcSalesPath = "input/sales_"+{{batchDate}}+".csv"
  val srcStoresPath = "input/stores_"+{{batchDate}}+".csv"

  //Load Transaction Sales file
  val transactionSalesData = spark.read.option("header", false).options(Map("delimiter"->",")).csv(srcSalesPath)

  //Load Stores file
  val StoresData = spark.read.option("header", false).options(Map("delimiter"->",")).csv(srcStoresPath)

  // Load Stores row from DB
  val StoresDb =  spark.read.jdbc(jdbcUrl, envProps.getString("mysql.table.stores"), connectionProperties)

  //Building columns from TransactionSales
  val salesDF = transactionSalesData
    .select(
      expr("uuid()").as("id"),
      col("_c0").as("store_token"),
      col("_c1").as("transaction_id"),
      trim(col("_c2")).as("receipt_token"),
      current_timestamp().as("created_datetime"),
      to_timestamp(col("_c3"),"yyyyMMdd'T'HHmmss.sss").as("transaction_datetime"),
      substring(col("_c4"), 2, 8).as("amount"),
      lit("USD").as("currency"),
      col("_c5").as("source_id"),
      col("_c6").as("user_role"),
      to_date(lit(batchDate),"yyyyMMdd").as("batch_date")
    )

  //Building columns from Stores
  val StoresDF = StoresData
      .select(
        expr("uuid()").as("id"),
        col("_c0").as("store_group"),
        trim(col("_c1")).as("store_token"),
        col("_c2").as("store_name"),
        current_timestamp().as("created_datetime")
      )

  //Removing duplicates
  val StoresUniqueDf = StoresDF.as("s")
      .join(StoresDb.as("db"), col("s.store_token")=== col("db.store_token"), "left")
      .where("db.store_token IS NULL")
      .select("s.*")

  //Save new Sales and Stores
  saveTransactionSales(salesDF)
  saveStores(StoresUniqueDf)

  //Move Processed Files
  moveFiles(srcSalesPath, envProps.getString("dts.history.path"), spark)
  moveFiles(srcStoresPath, envProps.getString("dts.history.path"), spark)

  //Remove Processed Files
  rmr(spark, srcSalesPath)
  rmr(spark, srcStoresPath)

  //End Process
  spark.close()
}
