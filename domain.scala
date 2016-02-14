
import sys.process._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

val hc = new org.apache.spark.sql.hive.HiveContext(sc) 
import hc.implicits._

  
// ********************************************************************
//  Convert CSV to TSV  
// ********************************************************************

def csv2Tsv(s0: String): String = {
	val s = s0.replaceAll("\t", " ")
	
	val line = if (s.indexOf("\"") > -1) {
	  val n1 = s.indexOf("\"")
	  val n2 = s.indexOf("\"", s.indexOf("\"") + 1)

	  val s1 = s.substring(0, n1).replaceAll(",", "\t")
	  val s2 = s.substring(n1 + 1, n2).replaceAll("\"", "") + "\t"
	  val s3 = s.substring(n2 + 1)

	  val ss = if (s3.indexOf("\"") > -1) s1 + s2 + csv2Tsv(s3)
	           else s1 + s2 + s3.replaceAll(",", "\t")

	  ss
    } else {
	  s.replaceAll(",", "\t")
    }

    line.toString
}


// *******************************************************
//  load & proceee csv file and then create RDD     
// *******************************************************

val domainRDD = sc.textFile("hdfs://10.120.42.10:8020/user/").map(csv2Tsv(_))


// ***********************************************************
// create DataFrame schema based on input file header line	
// ***********************************************************

def createSchema(): StructType = { 
   StructType( 
      Array( 
         StructField("DOMAIN_ID", StringType, false), 
         StructField("DOMAIN_NAME", StringType, false), 
         StructField("DOMAIN_CONCEPT_ID", StringType, false)
     ) 
   ) 
}


// ************************************************************
// transform a regular RDD to a Row RDD
// ************************************************************
	
val rowRDD = domainRDD.map(_.split("\t")). 
      map(p => Row ( 
                p(0), p(1), p(2) 
               ) 
         ) 

val domainDF  = hc.createDataFrame(domainRDD, createSchema()) 

domainDF.registerTempTable("tempTable") 

hc.sql("create table default.domain stored as parquet as select * from tempTable")

System.exit(0) 

