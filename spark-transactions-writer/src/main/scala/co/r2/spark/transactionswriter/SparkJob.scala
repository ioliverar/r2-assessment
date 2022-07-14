package co.r2.spark.transactionswriter

import co.r2.spark.transactionswriter.commons.Util
import org.apache.spark.sql.SparkSession

trait SparkJob {
  def sparkSessionCore(): SparkSession = {
    SparkSession
      .builder()
      .appName(appName)
      .master("local[1]")//comentar al desplegar en spark
      .config("spark.scheduler.mode", "FAIR")
      .config("spark.debug.maxToStringFields", 1000)
      .getOrCreate()
  }
  val spark = sparkSessionCore()

  def appName: String = Util.NAME_APP
}
