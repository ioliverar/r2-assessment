package co.r2.spark.transactionswriter.storage
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object DAOStorage extends MySQLServer {
  def readStores(spark: SparkSession): DataFrame = {
    spark.read.jdbc(jdbcUrl, envProps.getString("mysql.table.stores"), connectionProperties)
  }
  def saveTransactionSales(df: DataFrame): Unit ={
    df.write
      .mode(SaveMode.Append)
      .option("isolationLevel", "NONE")
      .jdbc(jdbcUrl, envProps.getString("mysql.table.transactions"), connectionProperties)
  }
  def saveStores(df: DataFrame): Unit ={
    df.write
      .mode(SaveMode.Append)
      .option("isolationLevel", "NONE")
      .option("deduplicate", "false")
      .jdbc(jdbcUrl, envProps.getString("mysql.table.stores"), connectionProperties)
  }
  def rmr(spark: SparkSession, path: String): Unit = {
    try {
      val fs = FileSystem.newInstance(spark.sparkContext.hadoopConfiguration)
      fs.delete(new Path(path), true)
      fs.close()
    } catch {
      case e: Throwable =>
    }
  }
  def moveFiles(srcPath: String, dstPath: String, spark: SparkSession): Unit = {
    try {
      val fs = FileSystem.newInstance(spark.sparkContext.hadoopConfiguration)
      fs.copyFromLocalFile(new Path(srcPath), new Path(dstPath))
      fs.close()
    } catch {
      case e: Throwable =>
    }
  }
}
