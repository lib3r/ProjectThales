import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import sqlContext.implicits._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import sys.process._
import org.apache.spark.sql._

import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.ml.Pipeline

object GdeltSandbox {

	def main(args: Array[String]) {
		// val conf = new SparkConf().setAppName(appName).setMaster(master)
		// val sc = new SparkContext(conf)

		// val data = sc.textFile("s3n://gdelt-em/data/*").cache()
		// val rowRDD = data.map(_.split("\t")).map(d => Row.fromSeq(d.toSeq))
		val sqlContext = new org.apache.spark.sql.SQLContext(sc)
		
		val eventsSchema = StructType(Array( StructField("GLOBALEVENTID",IntegerType,false), StructField("SQLDATE",IntegerType,true), StructField("MonthYear",IntegerType,true), StructField("Year",IntegerType,true), StructField("FractionDate",FloatType,true), StructField("Actor1Code",StringType,true), StructField("Actor1Name",StringType,true), StructField("Actor1CountryCode",StringType,true), StructField("Actor1KnownGroupCode",StringType,true), StructField("Actor1EthnicCode",StringType,true), StructField("Actor1Religion1Code",StringType,true), StructField("Actor1Religion2Code",StringType,true), StructField("Actor1Type1Code",StringType,true), StructField("Actor1Type2Code",StringType,true), StructField("Actor1Type3Code",StringType,true), StructField("Actor2Code",StringType,true), StructField("Actor2Name",StringType,true), StructField("Actor2CountryCode",StringType,true), StructField("Actor2KnownGroupCode",StringType,true), StructField("Actor2EthnicCode",StringType,true), StructField("Actor2Religion1Code",StringType,true), StructField("Actor2Religion2Code",StringType,true), StructField("Actor2Type1Code",StringType,true), StructField("Actor2Type2Code",StringType,true), StructField("Actor2Type3Code",StringType,true), StructField("IsRootEvent",StringType,true), StructField("EventCode",StringType,true), StructField("EventBaseCode",StringType,true), StructField("EventRootCode",StringType,true), StructField("QuadClass",StringType,true), StructField("GoldsteinScale",StringType,true), StructField("NumMentions",StringType,true), StructField("NumSources",StringType,true), StructField("NumArticles",StringType,true), StructField("AvgTone",StringType,true), StructField("Actor1Geo_Type",StringType,true), StructField("Actor1Geo_FullName",StringType,true), StructField("Actor1Geo_CountryCode",StringType,true), StructField("Actor1Geo_ADM1Code",StringType,true), StructField("Actor1Geo_Lat",StringType,true), StructField("Actor1Geo_Long",StringType,true), StructField("Actor1Geo_FeatureID",StringType,true), StructField("Actor2Geo_Type",StringType,true), StructField("Actor2Geo_FullName",StringType,true), StructField("Actor2Geo_CountryCode",StringType,true), StructField("Actor2Geo_ADM1Code",StringType,true), StructField("Actor2Geo_Lat",StringType,true), StructField("Actor2Geo_Long",StringType,true), StructField("Actor2Geo_FeatureID",StringType,true), StructField("ActionGeo_Type",StringType,true), StructField("ActionGeo_FullName",StringType,true), StructField("ActionGeo_CountryCode",StringType,true), StructField("ActionGeo_ADM1Code",StringType,true), StructField("ActionGeo_Lat",StringType,true), StructField("ActionGeo_Long",StringType,true), StructField("ActionGeo_FeatureID",StringType,true), StructField("DATEADDED",StringType,true), StructField("SOURCEURL",StringType,true)))			
			// val df = sqlContext.createDataFrame(rowRDD, eventsSchema)
		


		val dates = Array(20150612, 20150826, 20150126, 20150206, 20141009, 20141027, 20140616, 20140619, 20140410, 20140428, 20140219, 20140320, 20131204, 20140120, 20130912, 20131113, 20130529, 20130627, 20130206, 20130502, 20121022, 20121203, 20120910, 20120926, 20120504, 20120905, 20120302, 20120329)

		// val df = sqlContext.load("com.databricks.spark.csv", eventsSchema,Map("path" -> "s3n://gdelt-em/data/*", "header" -> "true", "delimiter" -> "\t"))

		val df = sqlContext.read.format("com.databricks.spark.csv").schema(eventsSchema).option("header","true").option("delimiter","\t").load("s3n://gdelt-em/data_test/*")

		val coder: (Int => Int) = (arg: Int) => {if (dates contains arg) 1 else 0}
		val labeler = udf(coder)

		val data = df.select(df("SQLDATE"),df("IsRootEvent"),df("EventCode"),df("EventBaseCode"),df("EventRootCode"),df("QuadClass"),df("GoldsteinScale"),df("NumMentions"),df("NumSources"),df("NumArticles"),df("AvgTone"))

		dataset.withColumn("Label",labeler(dataset.col("SQLDATE")))



		// Save CSV
		dataset.save("com.databricks.spark.csv", SaveMode.ErrorIfExists,Map("path" -> "s3n://gdelt-em/out/libsvm.csv","header"->"true"))
			

			// needs to be tab deliminated
			// val df = sqlContext.read.format("com.databricks.spark.csv").option("delimiter","\t").load("s3n://gdelt-em/data_test/*")
