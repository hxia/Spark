#!/usr/bin/perl

my $inputfile = $ARGV[0];
print "Input File: $inputfile \n";

open(IN, "$inputfile") || die "$! \n";

# *********************************************
#  define the field seperator used in sourcce
#  ********************************************
#my $fieldSeperator = "\\\|";
#my $fieldSeperator = "~";
my $fieldSeperator = "\t";

my $lineNo = 0;

my $tableName = $inputfile;
$tableName =~ s/\.txt//;
$tableName =~ s/\.csv//;
$tableName =~ s/\.dat//;

open(OUT, ">$tableName.scala") || die "$! \n";

print OUT <<Header;

import sys.process._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

val hc = new org.apache.spark.sql.hive.HiveContext(sc) 
import hc.implicits._


// ********************************************************************
//  replace TAB to whitespace and then convert CSV to TSV format 
// ********************************************************************

def csv2Tsv(s0: String, fieldDelimiter: String): String = {
    //val s = s0.replaceAll("\\t", " ")
    val s = s0

    val line = if (s.indexOf("\\"") > -1) {
      val n1 = s.indexOf("\\"")
      val n2 = s.indexOf("\\"", s.indexOf("\\"") + 1)

      val s1 = s.substring(0, n1)
      val s2 = s.substring(n1 + 1, n2)
      val s3 = s.substring(n2 + 1)

      //val ss = if (s3.indexOf("\\"") > -1) s1.replaceAll(",", fieldDelimiter) + s2.replace("\\"", "") + csv2Tsv(s3, fieldDelimiter)
      //else s1.replaceAll(",", fieldDelimiter) + s2.replace("\\"", "") + s3.replaceAll(",", fieldDelimiter)
      val ss = if (s3.indexOf("\\"") > -1) s1 + s2.replace("\\"", "") + csv2Tsv(s3, fieldDelimiter)
      else s1 + s2.replace("\\"", "") + s3

      ss
    } else {
      s.replaceAll(",", fieldDelimiter)
    }

    line.toString
}


// *******************************************************
//  load & proceee csv file and then create RDD     
// *******************************************************

Header


my $rdd = $tableName."RDD";
my $df = $tableName."DF";

my $hdfsFile = "hdfs://10.120.42.10:8020/user/$inputfile";
print "HDFS File: $hdfsFile \n";


# add a leading \ for escaped char
   #print OUT "val $rdd = sc.textFile(\"$hdfsFile\").map(r => r + \",NULL\").map(csv2Tsv(_, \"   \"))\n";
if($fieldSeperator =~ /\t/){
   # handle tab delimited files
   print OUT "val $rdd = sc.textFile(\"$hdfsFile\").map(r => r + \",NULL\").map(csv2Tsv(_, \"\\t\"))\n";
}elsif($fieldSeperator =~ /\\/){
   # handle the field delimiter need escaped
   print OUT "val $rdd = sc.textFile(\"$hdfsFile\").map(r => r + \",NULL\").map(csv2Tsv(_, \"\\$fieldSeperator\"))\n";
}else{
   # handle the normal field delimiter, which don't need ito be escaped
   print OUT "val $rdd = sc.textFile(\"$hdfsFile\").map(r => r + \",NULL\").map(csv2Tsv(_, \"$fieldSeperator\"))\n";
}

my $headerLine = `head -n 1 $inputfile`;
$headerLine =~ s/\r//g;
$headerLine =~ s/\n//g;

print "Header:$headerLine\n";

#my @header = split(",",  $headerLine);
my @header = split($fieldSeperator,  $headerLine);
print "Num of Columns: " + @header + "\n";
 
print OUT "\n";
print OUT "def createSchema(): StructType = { \n";
print OUT "  StructType( \n";
print OUT "     Array( \n";

for(my $i=0; $i < @header-1; $i++){

   print OUT <<SCHEMA;
        StructField("$header[$i]", StringType, false),
SCHEMA
} 

print OUT "        StructField(\"$header[@header-1]\", StringType, false)\n";
print OUT "     ) \n";
print OUT "   ) \n";
print OUT "} \n\n";



# *******************************************************
#   convert a regular RDD to a Row RDD
# *******************************************************

# add a leading \ for escaped char
if($fieldSeperator =~ /\\/){
   # handle the field delimiter needed to be escaped
   #print OUT "val rowRDD = $rdd.map(_.split(\"   \")). \n";
   print OUT "val rowRDD = $rdd.map(_.split(\"\\$fieldSeperator\")). \n";
} elsif($fieldSeperator =~ /\t/) {
   # handle tab delimited fiels
   print OUT "val rowRDD = $rdd.map(_.split(\"\\t\")). \n";
} else {
   print OUT "val rowRDD = $rdd.map(_.split(\"$fieldSeperator\")). \n";
}

print OUT "     map(p => Row ( \n";


my $n = @header;
for(my $j=0; $j < $n-1; $j++){
   if(($j % 10 == 0) && ($j > 0)) {
       print OUT "\n           p($j), ";
   } else {
       if($j ==0) {
          print OUT "           p($j), ";
       } else {
          print OUT "p($j), ";
       }
   }
}
my $m = $n - 1;
print OUT "p($m) \n";

print OUT "       ) \n";
print OUT "  ) \n\n";

print OUT "val $df  = hc.createDataFrame(rowRDD, createSchema()) \n";


print OUT "$df.registerTempTable(\"tempTable\") \n\n";
#print OUT "hc.sql(\"create table omop.$tableName stored as parquet as select * from tempTable\") \n\n";
print OUT "hc.sql(\"create table $tableName stored as parquet as select * from tempTable\") \n\n";
print OUT "System.exit(0) \n\n";


my $cmd1 = "hdfs dfs -put $inputfile /user/$inputfile";
print "Command = \"$cmd1\" \n";
system($cmd1);


my $cmd = "/usr/bin/spark-shell -i $tableName.scala";
print "Command = $cmd \n";
system($cmd);


my $cmd2 = "hdfs dfs -rm /user/$inputfile";
print "Command = $cmd2 \n";
system($cmd2);

