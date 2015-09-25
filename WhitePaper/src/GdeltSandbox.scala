import sqlContext.implicits._
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import  org.apache.spark.sql.types._
import org.apache.spark.sql.functions._



object GdeltSandbox {

	def main(args: Array[String]) {
			// val conf = new SparkConf().setAppName(appName).setMaster(master)
			// val sc = new SparkContext(conf)
			val sqlContext = new org.apache.spark.sql.SQLContext(sc)

			val eventsSchema = StructType(Array( StructField("GLOBALEVENTID",IntegerType,false), StructField("SQLDATE",IntegerType,true), StructField("MonthYear",IntegerType,true), StructField("Year",IntegerType,true), StructField("FractionDate",FloatType,true), StructField("Actor1Code",StringType,true), StructField("Actor1Name",StringType,true), StructField("Actor1CountryCode",StringType,true), StructField("Actor1KnownGroupCode",StringType,true), StructField("Actor1EthnicCode",StringType,true), StructField("Actor1Religion1Code",StringType,true), StructField("Actor1Religion2Code",StringType,true), StructField("Actor1Type1Code",StringType,true), StructField("Actor1Type2Code",StringType,true), StructField("Actor1Type3Code",StringType,true), StructField("Actor2Code",StringType,true), StructField("Actor2Name",StringType,true), StructField("Actor2CountryCode",StringType,true), StructField("Actor2KnownGroupCode",StringType,true), StructField("Actor2EthnicCode",StringType,true), StructField("Actor2Religion1Code",StringType,true), StructField("Actor2Religion2Code",StringType,true), StructField("Actor2Type1Code",StringType,true), StructField("Actor2Type2Code",StringType,true), StructField("Actor2Type3Code",StringType,true), StructField("IsRootEvent",StringType,true), StructField("EventCode",StringType,true), StructField("EventBaseCode",StringType,true), StructField("EventRootCode",StringType,true), StructField("QuadClass",StringType,true), StructField("GoldsteinScale",StringType,true), StructField("NumMentions",StringType,true), StructField("NumSources",StringType,true), StructField("NumArticles",StringType,true), StructField("AvgTone",StringType,true), StructField("Actor1Geo_Type",StringType,true), StructField("Actor1Geo_FullName",StringType,true), StructField("Actor1Geo_CountryCode",StringType,true), StructField("Actor1Geo_ADM1Code",StringType,true), StructField("Actor1Geo_Lat",StringType,true), StructField("Actor1Geo_Long",StringType,true), StructField("Actor1Geo_FeatureID",StringType,true), StructField("Actor2Geo_Type",StringType,true), StructField("Actor2Geo_FullName",StringType,true), StructField("Actor2Geo_CountryCode",StringType,true), StructField("Actor2Geo_ADM1Code",StringType,true), StructField("Actor2Geo_Lat",StringType,true), StructField("Actor2Geo_Long",StringType,true), StructField("Actor2Geo_FeatureID",StringType,true), StructField("ActionGeo_Type",StringType,true), StructField("ActionGeo_FullName",StringType,true), StructField("ActionGeo_CountryCode",StringType,true), StructField("ActionGeo_ADM1Code",StringType,true), StructField("ActionGeo_Lat",StringType,true), StructField("ActionGeo_Long",StringType,true), StructField("ActionGeo_FeatureID",StringType,true), StructField("DATEADDED",StringType,true), StructField("SOURCEURL",StringType,true)))

			val df = sqlContext.load("com.databricks.spark.csv", eventsSchema,Map("path" -> "s3n://gdelt-em/euromaidan/*", "header" -> "true", "delimiter" -> "\t"))


			// Hack datetime issues
			val d = DateTimeFormat.forPattern("yyyy-mm-dd'T'kk:mm:ssZ")
			val dtFunc: (String => Date) = (arg1: String) => DateTime.parse(arg1, d).toDate
			val x = df.withColumn("dt", callUDF(dtFunc, DateType, col("dt_string"))

			// Save CSV
			df.save("com.databricks.spark.csv", SaveMode.ErrorIfExists,Map("path" -> "s3n://gdelt-em/output/","header"->"true"))
			

			// needs to be tab deliminated
			// val df = sqlContext.read.format("com.databricks.spark.csv").option("delimiter","\t").load("s3n://gdelt-em/data_test/*")

	def convertColumn(df: org.apache.spark.sql.DataFrame, name:String, newType:String) = {
		val df_1 = df.withColumnRenamed(name, "swap")
		df_1.withColumn(name, df_1.col("swap").cast(newType)).drop("swap")
	}

	def getTotalSize(rdd: RDD[Row]): Long = {
	  // This can be a parameter
			val NO_OF_SAMPLE_ROWS = 10l;
			val totalRows = rdd.count();
			var totalSize = 0l
			if (totalRows > NO_OF_SAMPLE_ROWS) {
				val sampleRDD = rdd.sample(true, NO_OF_SAMPLE_ROWS)
				val sampleRDDSize = getRDDSize(sampleRDD)
				totalSize = sampleRDDSize.*(totalRows)./(NO_OF_SAMPLE_ROWS)
			} else {
				// As the RDD is smaller than sample rows count, we can just calculate the total RDD size
				totalSize = getRDDSize(rdd)
			}

			totalSize
	}

	def getRDDSize(rdd: RDD[Row]) : Long = {
	    var rddSize = 0l
	    val rows = rdd.collect()
	    for (i <- 0 until rows.length) {
	       rddSize += SizeEstimator.estimate(rows.apply(i).toSeq.map { value => value.toString() })
	    }

	    rddSize
	}