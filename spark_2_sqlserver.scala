
import sys.process._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

val hc = new org.apache.spark.sql.SQLContext(sc)
import hc.implicits._

// define JDBC connection for SQL Server
val url = "jdbc:sqlserver://10.120.42.95:1436;user=rdcDemoApp;password=Del01tte!;databaseName=omop;sendStringParametersAsUnicode=false"

//val deGPLInfo = sqlContext.load("jdbc", Map("url" -> "jdbc:oracle:thin:deapp/deapp@//10.120.42.112:1521/tsmrt", "dbtable" -> "deapp.de_gpl_info"))

val domainRDD = sc.textFile("hdfs://10.120.42.10:8020/user/DOMAIN.csv").map(r => r + "\tNULL")

def createSchema(): StructType = {
  StructType(
     Array(
        StructField("DOMAIN_ID", StringType, false),
        StructField("DOMAIN_NAME", StringType, false),
        StructField("DOMAIN_CONCEPT_ID", StringType, false)
		//StructField("DOMAIN_CONCEPT_ID", IntegerType, false)
     )
   )
}

val rowRDD = domainRDD.map(_.split("\t")).
     map(p => Row (
           p(0), p(1), p(2)
       )
  )

val domainDF  = hc.createDataFrame(rowRDD, createSchema())

//domainDF.createJDBCTable(url, "domain", true)

domainDF.insertIntoJDBC(url, "domain", false)

domainDF.map(x => Row(x.get(1).toString)).collect().foreach(println)

