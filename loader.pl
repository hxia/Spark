#!/usr/bin/perl

&main();

sub main(){

	my $databaseName = "default";
	
	# *********************************************
	#  define the field seperator used in sourcce
	#  ********************************************
	#my $fieldSeperator = "\\\|";
	my $fieldSeperator = "~";
	#my $fieldSeperator = "\t";
	#my $fieldSeperator = ",";
	print "Field Seperator: $fieldSeperator \n";

	# *********************************************
	#  extract filename from commandline input
	#  ********************************************
	my $inputfile = $ARGV[0];
	print "Input File: $inputfile \n";

	my $headerLine = `head -n 1 $inputfile`;
	my @header = &getHeaderLine($headerLine, "\\$fieldSeperator");


	# create tablename based on input filename
	my $tableName = lc($inputfile);
	$tableName =~ s/\.txt//;
	$tableName =~ s/\.csv//;
	$tableName =~ s/\.dat//;
	print "Table Name: $tableName \n";

	
	# *************************************************************
	# create Scala file for Spark to run
 	# *************************************************************
	
	open(OUT, ">$tableName.scala") || die "$! \n";
	print OUT &getHeader();
	print OUT &selectLineParser($fieldSeperator);
	print OUT &createRDD($inputfile, $tableName, $fieldSeperator);
	print OUT &getSchema(@header);
	print OUT &transformRDD($tableName, @header);
	print OUT &convertRDD2Impala($tableName, $databaseName);
	close(OUT);
	
	
	# *************************************************************
	#  preparation and cleanup
 	# *************************************************************
	
	&loadFile2HDFS($inputfile);
	&startSpark($tableName);
	&deleteHDFSFile($inputfile);
	&deleteScalaFile($tableName);
}


# ************************************************************************************
#  selectLineParser: generate a correct parser to parse input file's each line,
#                    currently support CSV and TSV format
# ************************************************************************************

sub selectLineParser(){
   my $fieldSeperator = $_[0];
   
   if($fieldSeperator =~ /,/){
      return &getCsv2Tsv();
   } elsif($fieldSeperator =~ /\t/){
      return "";
   } elsif($fieldSeperator =~ /\\/){
      return "";
   } else {
      return  &getDelimited2Tsv($fieldSeperator);
   }
}


# ************************************************************************************
#  create import statments for Scala
# ************************************************************************************

sub getHeader(){

   my $header = <<HEADER;

import sys.process._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

val hc = new org.apache.spark.sql.hive.HiveContext(sc) 
import hc.implicits._

HEADER

   return $header;
}


# ************************************************************************************
#  getCsv2Tsv: create a line parser to convert CSV to TSV 
# ************************************************************************************

sub getCsv2Tsv(){

  my $lineParser = <<LINE;
  
// ********************************************************************
//  Convert CSV to TSV  
// ********************************************************************

def csv2Tsv(s0: String): String = {
	val s = s0.replaceAll("\\t", " ")
	
	val line = if (s.indexOf("\\"") > -1) {
	  val n1 = s.indexOf("\\"")
	  val n2 = s.indexOf("\\"", s.indexOf("\\"") + 1)

	  val s1 = s.substring(0, n1).replaceAll(",", "\\t")
	  val s2 = s.substring(n1 + 1, n2).replaceAll("\\"", "") + "\\t"
	  val s3 = s.substring(n2 + 1)

	  val ss = if (s3.indexOf("\\"") > -1) s1 + s2 + csv2Tsv(s3)
	           else s1 + s2 + s3.replaceAll(",", "\\t")

	  ss
    } else {
	  s.replaceAll(",", "\\t")
    }

    line + "\\tNULL"
}

LINE

}


# ************************************************************************************
#  getDelimited2Tsv: create a line parser to convert delimited to TSV file 
# ************************************************************************************

sub getDelimited2Tsv(){
	my $fieldDelimiter =$_[0];
	
    my $cmd = <<LINE;
  
// ********************************************************************
//  replace TAB to whitespace and then convert delimited to TSV format 
// ********************************************************************

def delimited2Tsv(s: String, fieldDelimiter: String): String = {    
      s.replaceAll("\\t", " ").replaceAll(\"$fieldDelimiter\", "\\t")
}

LINE

	return $cmd;
}


# ************************************************************************************
#  createRDD: 
# ************************************************************************************

sub createRDD(){

	my $inputfile = $_[0];
	my $tableName = $_[1];
	my $fieldSeperator = $_[2];
	
	my $cmd = <<COMMENT;

// *******************************************************
//  load & proceee csv file and then create RDD     
// *******************************************************

COMMENT

	my $hdfsFile = "hdfs://10.120.42.10:8020/user/$inputfile";
	my $rdd = $tableName."RDD";

	print "RDD: $rdd \n";
	print "HDFS File: $hdfsFile \n";

	# add a leading \ for escaped char
	if($fieldSeperator =~ /\t/){
	   # handle TAB delimited files
	   $cmd .= "val $rdd = sc.textFile(\"$hdfsFile\")\n\n";
	}elsif($fieldSeperator =~ /\\/){
	   # handle the field delimiter need escaped
	   # print OUT "val $rdd = sc.textFile(\"$hdfsFile\").map(r => r + \",NULL\").map(csv2Tsv(_, \"\\$fieldSeperator\"))\n";
	   $cmd .= "val $rdd = sc.textFile(\"$hdfsFile\").map(delimited2Tsv(_, \"\\$fieldSeperator\"))\n\n";
	}elsif($fieldSeperator =~ /,/){
	   # handle CSV format files
	   $cmd .= "val $rdd = sc.textFile(\"$hdfsFile\").map(csv2Tsv(_))\n\n";
	} else {
	   $cmd .= "val $rdd = sc.textFile(\"$hdfsFile\").map(delimited2Tsv(_, \"\\$fieldSeperator\"))\n\n";
	}

	return $cmd;
}


# ************************************************************************
#   getHeaderLine: parse the txt file's header line
# ************************************************************************

sub getHeaderLine(){
   my $headerLine = $_[0];
   my $fieldSeperator = $_[1];
	
   $headerLine =~ s/\r//g;
   $headerLine =~ s/\n//g;   
   #print "Header:   $headerLine\n";
   #print "Field Seperator: $fieldSeperator \n";

   my @header = split($fieldSeperator,  $headerLine);
   my $n = @header;
   print "Num of Columns:  $n \n";

   for(my $i=0; $i < @header; $i++){
     print "$i \t $header[$i] \n";
   }

   return @header;   
}


# ************************************************************************
#   getSchema: build DataFrame schema based on txt file header line
# ************************************************************************
 
sub getSchema(){ 
    my @header = @_;  
   
    my $schema = <<START_SCHEMA;

// ***********************************************************
// create DataFrame schema based on input file header line	
// ***********************************************************

def createSchema(): StructType = { 
   StructType( 
      Array( 
START_SCHEMA

	for(my $i=0; $i < @header-1; $i++){
		$schema .= "         StructField(\"$header[$i]\", StringType, false), \n";
	} 

    $schema .= <<END_SCHEMA;
         StructField("$header[@header-1]", StringType, false)
     ) 
   ) 
}

END_SCHEMA

   return $schema;
}


# *******************************************************
#   transformRDD: transform a regular RDD to a Row RDD
# *******************************************************

sub transformRDD(){
	my ($tableName, @header) = @_;
	
	my $rdd = $tableName."RDD";
	my $df = $tableName."DF";

	my $cmd = <<MSG;

// ************************************************************
// transform a regular RDD to a Row RDD
// ************************************************************
	
MSG

	$cmd .= "val rowRDD = $rdd.filter(_.indexOf(\"$header[0]\") < 0).map(r => r + \"\\tNULL\").map(_.split(\"\\t\")). \n";
	$cmd .= "      map(p => Row ( \n";

	my $n = @header;
	for(my $j=0; $j < $n-1; $j++){
	   if(($j % 10 == 0) && ($j > 0)) {
		   $cmd .= "\n           p($j), ";
	   } else {
		   if($j ==0) {
			  $cmd .= "                p($j), ";
		   } else {
			  $cmd .= "p($j), ";
		   }
	   }
	}
	
	my $m = $n - 1;
	$cmd .= "p($m) \n";
	$cmd .= "               ) \n";
	$cmd .= "         ) \n\n";

	return $cmd;
}


# *******************************************************
#   convertRDD2Impala:  load the raw file into HDFS
# *******************************************************

sub convertRDD2Impala() {

    my $tableName = $_[0];
	my $databaseName = $_[1];
	
	my $rdd = $tableName."RDD";
	my $df = $tableName."DF";

	my $cmd = "val $df  = hc.createDataFrame(rowRDD, createSchema()) \n\n";
    $cmd .= "$df.registerTempTable(\"tempTable\") \n\n";
	$cmd .= "hc.sql(\"create table $databaseName.$tableName stored as parquet as select * from tempTable\")\n\n";
	$cmd .= "System.exit(0) \n\n";
	
   return $cmd;
}


# *******************************************************
#   loadRawFile:  load the raw file into HDFS
# *******************************************************

sub loadFile2HDFS(){
	my $inputfile = $_[0];
	my $cmd = "hdfs dfs -put $inputfile /user/$inputfile";
	print "Command = $cmd \n";
	system($cmd);
}


# *******************************************************
#   startSpark: start Spark session
# *******************************************************

sub startSpark(){
	my $scalaFile = $_[0];
	my $cmd = "/usr/bin/spark-shell -i $scalaFile.scala";
	print "Command = $cmd \n";
	system($cmd);
}


# ********************************************************
#   deleteScalaFile: delete created Scala file for Spark
# ********************************************************

sub deleteScalaFile(){
	my $scalaFile = $_[0];
	unlink "$scalaFile.scala";
}


# *******************************************************
#   deleteRawFile: delete the raw file from HDFS
# *******************************************************

sub deleteHDFSFile(){
    my $inputfile = $_[0];
	my $cmd = "hdfs dfs -rm /user/$inputfile";
	print "Command = $cmd \n";
	system($cmd);
}

