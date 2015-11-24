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

	def evaluate() = {
		// Instantiate metrics object
	    val metrics = new MulticlassMetrics(predictionAndLabels)

	    // Confusion matrix
	    println("Confusion matrix:")
	    println(metrics.confusionMatrix)

	    // Overall Statistics
	    val precision = metrics.precision
	    val recall = metrics.recall // same as true positive rate
	    val f1Score = metrics.fMeasure
	    println("Summary Statistics")
	    println(s"Precision = $precision")
	    println(s"Recall = $recall")
	    println(s"F1 Score = $f1Score")

	    // Precision by label
	    val labels = metrics.labels
	    labels.foreach { l =>
	      println(s"Precision($l) = " + metrics.precision(l))
	    }

	    // Recall by label
	    labels.foreach { l =>
	      println(s"Recall($l) = " + metrics.recall(l))
	    }

	    // False positive rate by label
	    labels.foreach { l =>
	      println(s"FPR($l) = " + metrics.falsePositiveRate(l))
	    }

	    // F-measure by label
	    labels.foreach { l =>
	      println(s"F1-Score($l) = " + metrics.fMeasure(l))
	    }

	    // Weighted stats
	    println(s"Weighted precision: ${metrics.weightedPrecision}")
	    println(s"Weighted recall: ${metrics.weightedRecall}")
	    println(s"Weighted F1 score: ${metrics.weightedFMeasure}")
	    println(s"Weighted false positive rate: ${metrics.weightedFalsePositiveRate}")

	}

	def event_pipeline(dataset) = {
	    """
	    """
	    val EventCodeI = new StringIndexer()
	    	.setInputCol("EventCode")
	    	.setOutputCol("EventCodeI")

	    val EventBaseCodeI = new StringIndexer()
	    	.setInputCol("EventBaseCode")
	    	.setOutputCol("EventBaseCodeI")

	    val EventRootCodeI = new StringIndexer().
	    	.setInputCol("EventRootCode")
	    	.setOutputCol("EventRootCodeI")
	    
	    val assembler = new VectorAssembler()
	    	.setInputCols(Array("IsRootEvent", "EventCodeI", "EventBaseCodeI","EventRootCodeI", "QuadClass","GoldsteinScale","NumMentions","NumSources","NumArticles","AvgTone")
	    	.setOutputCol("features")
	    
	    val featureIndexer = new VectorIndexer()
	    	.setInputCol("features")
	    	.setOutputCol("indexedFeatures")
	    	.setMaxCategories(310)

	    val pipeline = new Pipeline()
	    	.setStages(Array(EventCodeI, EventBaseCodeI, EventRootCodeI, assembler,featureIndexer))
	    
	    val model = pipeline.fit(dataset)
	    val output = model.transform(dataset)

	    val data = output.map(row => LabeledPoint(row(0), row.last).cache()
	    print "Data:"
	    print data.take(1)
    }

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
		df.withColumn("Label",labeler(dataset.col("SQLDATE")))

		val dataset = df.select(df("SQLDATE"),df("IsRootEvent"),df("EventCode"),df("EventBaseCode"),df("EventRootCode"),df("QuadClass"),df("GoldsteinScale"),df("NumMentions"),df("NumSources"),df("NumArticles"),df("AvgTone"))

		data = event_pipeline(dataset)

	}

	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("GDT-mllib")
		val sc = new SparkContext(conf)

		val sqlContext = new org.apache.spark.sql.SQLContext(sc)
	
		val filename = "s3n://gdelt-em/data/20150824.export.CSV"
		val df = sqlContext.read.format("com.databricks.spark.csv").schema(eventsSchema).option("header","true").option("delimiter","\t").load(filename)

		(data, categoricalFeaturesInfo, labels) = preprocess(df)





		// Save CSV
		dataset.save("com.databricks.spark.csv", SaveMode.ErrorIfExists,Map("path" -> "s3n://gdelt-em/out/libsvm.csv","header"->"true"))


		
