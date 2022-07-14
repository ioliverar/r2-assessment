package co.r2.spark.transactionswriter.storage

import java.sql.{Connection, DriverManager}
import java.util.Properties

import com.typesafe.config.ConfigFactory

trait MySQLServer {
  val conf = ConfigFactory.load()
  val envProps = conf.getConfig("dev") //getConfig(args(0))

  val DBHostname    = envProps.getString("mysql.jdbc.hostname")
  val DBPort        = envProps.getString("mysql.jdbc.port")
  val DBDatabase    = envProps.getString("mysql.jdbc.database")
  val DBUsername    = envProps.getString("mysql.jdbc.username")
  val DBPassword    = envProps.getString("mysql.jdbc.password")
  val DBDriverClass = envProps.getString("mysql.jdbc.driver")

  val jdbcUrl = s"jdbc:mysql://${DBHostname}:${DBPort}/${DBDatabase}"

  val connectionProperties = new Properties()
  connectionProperties.put("user", s"${DBUsername}")
  connectionProperties.put("password", s"${DBPassword}")
  connectionProperties.setProperty("Driver", DBDriverClass)
  connectionProperties.setProperty("AutoCommit", "true")
  connectionProperties.setProperty("useSSL", "false")
  connectionProperties.setProperty("useServerPrepStmts", "true")
  connectionProperties.setProperty("cachePrepStmts", "true")
  connectionProperties.setProperty("useLocalSessionState", "true")
  connectionProperties.setProperty("prepStmtCacheSize", "250")
  connectionProperties.setProperty("prepStmtCacheSqlLimit", "2048")

  var connection : Connection = _
  Class.forName(DBDriverClass)
  connection = DriverManager.getConnection(jdbcUrl, DBUsername, DBPassword)
}
