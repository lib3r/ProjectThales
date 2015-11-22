import org.apache.spark.ml.classification.OneVsRest
import org.apache.spark.mllib.classification.SVM
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import sqlContext.implicits._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import sys.process._

object GdeltMultiClassSVM {

	val eventsSchema = StructType(Array(
	StructField("GLOBALEVENTID",IntegerType,false),
	StructField("SQLDATE",IntegerType,true),
	StructField("MonthYear",IntegerType,true),
	StructField("Year",IntegerType,true),
	StructField("FractionDate",FloatType,true),
	StructField("Actor1Code",StringType,true),
	StructField("Actor1Name",StringType,true),
	StructField("Actor1CountryCode",StringType,true),
	StructField("Actor1KnownGroupCode",StringType,true),
	StructField("Actor1EthnicCode",StringType,true),
	StructField("Actor1Religion1Code",StringType,true),
	StructField("Actor1Religion2Code",StringType,true),
	StructField("Actor1Type1Code",StringType,true),
	StructField("Actor1Type2Code",StringType,true),
	StructField("Actor1Type3Code",StringType,true),
	StructField("Actor2Code",StringType,true),
	StructField("Actor2Name",StringType,true),
	StructField("Actor2CountryCode",StringType,true),
	StructField("Actor2KnownGroupCode",StringType,true),
	StructField("Actor2EthnicCode",StringType,true),
	StructField("Actor2Religion1Code",StringType,true),
	StructField("Actor2Religion2Code",StringType,true),
	StructField("Actor2Type1Code",StringType,true),
	StructField("Actor2Type2Code",StringType,true),
	StructField("Actor2Type3Code",StringType,true),
	StructField("IsRootEvent",StringType,true),
	StructField("EventCode",StringType,true),
	StructField("EventBaseCode",StringType,true),
	StructField("EventRootCode",StringType,true),
	StructField("QuadClass",StringType,true),
	StructField("GoldsteinScale",StringType,true),
	StructField("NumMentions",StringType,true),
	StructField("NumSources",StringType,true),
	StructField("NumArticles",StringType,true),
	StructField("AvgTone",StringType,true),
	StructField("Actor1Geo_Type",StringType,true),
	StructField("Actor1Geo_FullName",StringType,true),
	StructField("Actor1Geo_CountryCode",StringType,true),
	StructField("Actor1Geo_ADM1Code",StringType,true),
	StructField("Actor1Geo_Lat",StringType,true),
	StructField("Actor1Geo_Long",StringType,true),
	StructField("Actor1Geo_FeatureID",StringType,true),
	StructField("Actor2Geo_Type",StringType,true),
	StructField("Actor2Geo_FullName",StringType,true),
	StructField("Actor2Geo_CountryCode",StringType,true),
	StructField("Actor2Geo_ADM1Code",StringType,true),
	StructField("Actor2Geo_Lat",StringType,true),
	StructField("Actor2Geo_Long",StringType,true),
	StructField("Actor2Geo_FeatureID",StringType,true),
	StructField("ActionGeo_Type",StringType,true),
	StructField("ActionGeo_FullName",StringType,true),
	StructField("ActionGeo_CountryCode",StringType,true),
	StructField("ActionGeo_ADM1Code",StringType,true),
	StructField("ActionGeo_Lat",StringType,true),
	StructField("ActionGeo_Long",StringType,true),
	StructField("ActionGeo_FeatureID",StringType,true),
	StructField("DATEADDED",StringType,true),
	StructField("SOURCEURL",StringType,true)))	

	val dates = Array(
	20150817, 20150723, 20150612, 20150427, 20150126, 20150107, 20141009, 
	20140819, 20140616, 20140410, 20140219, 20131204, 20131014, 20130711, 
	20130529, 20130419, 20130206, 20121022, 20120910, 20120809, 20120504, 
	20120302, 20150826, 20150803, 20150708, 20150507, 20150206, 20150119, 
	20141027, 20140828, 20140619, 20140519, 20140320, 20140120, 20131113, 
	20130729, 20130627, 20130502, 20130415, 20121203, 20120926, 20120905, 
	20120731, 20120329)

	val maxima = Array(
	20150817, 20150723, 20150612, 20150427, 20150126, 20150107, 20141009,
	20140819, 20140616, 20140410, 20140219, 20131204, 20131014, 20130711, 
	20130529, 20130419, 20130206, 20121022, 20120910, 20120809, 20120504, 
	20120302)

	val minima = Array(
	20150826, 20150803, 20150708, 20150507, 20150206, 20150119, 20141027, 
	20140828, 20140619, 20140519, 20140320, 20140120, 20131113, 20130729, 
	20130627, 20130502, 20130415, 20121203, 20120926, 20120905, 20120731, 
	20120329)

	def preprocess(df: DataFrame) = {
		"""
		Convert GDELT files into a format for DecisionTree
		"""
		val df = df.filter("EventRootCode != '--'")
    	val df = df.filter("EventRootCode != 'X'")


	    val categoricalFeaturesInfo = Map(
	        0 -> df.select("IsRootEvent").distinct().count(), 
	        1 -> df.select("EventCode").distinct().count(),
	        2 -> df.select("EventBaseCode").distinct().count(),
	        3 -> df.select("EventRootCode").distinct().count(),
	        4 -> df.select("QuadClass").distinct().count())

		val coder: (Int => Float) = (dates: Int) => {
			if (dates contains maxima) {return 1.0}
			else if (dates contains minima) {return 2.0}
			else {return 0.0}
		}
		val labeler = udf(coder)

		val data = df.select(df("SQLDATE"),df("IsRootEvent"),df("EventCode"),df("EventBaseCode"),df("EventRootCode"),df("QuadClass"),df("GoldsteinScale"),df("NumMentions"),df("NumSources"),df("NumArticles"),df("AvgTone"))

		dataset.withColumn("Label",labeler(dataset.col("SQLDATE")))

	}

	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("GdeltMultiClassSVM")
		val sc = new SparkContext(conf)

		val sqlContext = new SQLContext(sc)
		val dataPath = "s3n://gdelt-em/data_test/*"
		val df = sqlContext.read.format("com.databricks.spark.csv").schema(eventsSchema).option("header","true").option("delimiter","\t").load(dataPath)
	
		(data, categoricalFeaturesInfo, labels) = preprocess(df)
		val Array(train, test) = data.randomSplit(Array(0.7, 0.3))

		val ovr = new OneVsRest().setClassifier(new SVM)
		val ovrModel = ovr.fit(train)

		val predictions = ovrModel.transform(test).select("prediction", "Label")
		val predictionsAndLabels = predictions.map {case Row(p: Double, l: Double) => (p, l)}
	}

}

