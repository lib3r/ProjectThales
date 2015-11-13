from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import udf

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint

eventsSchema = StructType([
    StructField("GLOBALEVENTID",IntegerType(),False), 
    StructField("SQLDATE",IntegerType(),False), 
    StructField("MonthYear",IntegerType(),False), 
    StructField("Year",IntegerType(),False), 
    StructField("FractionDate",FloatType(),False), 
    StructField("Actor1Code",StringType(),True), 
    StructField("Actor1Name",StringType(),True), 
    StructField("Actor1CountryCode",StringType(),True), 
    StructField("Actor1KnownGroupCode",StringType(),True), 
    StructField("Actor1EthnicCode",StringType(),True), 
    StructField("Actor1Religion1Code",StringType(),True), 
    StructField("Actor1Religion2Code",StringType(),True), 
    StructField("Actor1Type1Code",StringType(),True), 
    StructField("Actor1Type2Code",StringType(),True), 
    StructField("Actor1Type3Code",StringType(),True), 
    StructField("Actor2Code",StringType(),True), 
    StructField("Actor2Name",StringType(),True), 
    StructField("Actor2CountryCode",StringType(),True), 
    StructField("Actor2KnownGroupCode",StringType(),True), 
    StructField("Actor2EthnicCode",StringType(),True), 
    StructField("Actor2Religion1Code",StringType(),True), 
    StructField("Actor2Religion2Code",StringType(),True), 
    StructField("Actor2Type1Code",StringType(),True), 
    StructField("Actor2Type2Code",StringType(),True), 
    StructField("Actor2Type3Code",StringType(),True), 
    StructField("IsRootEvent",StringType(),True), 
    StructField("EventCode",StringType(),True), 
    StructField("EventBaseCode",StringType(),True), 
    StructField("EventRootCode",StringType(),True), 
    StructField("QuadClass",StringType(),True), 
    StructField("GoldsteinScale",FloatType(),True), 
    StructField("NumMentions",IntegerType(),True), 
    StructField("NumSources",IntegerType(),True), 
    StructField("NumArticles",IntegerType(),True), 
    StructField("AvgTone",FloatType(),True), 
    StructField("Actor1Geo_Type",StringType(),True), 
    StructField("Actor1Geo_FullName",StringType(),True), 
    StructField("Actor1Geo_CountryCode",StringType(),True), 
    StructField("Actor1Geo_ADM1Code",StringType(),True), 
    StructField("Actor1Geo_Lat",StringType(),True), 
    StructField("Actor1Geo_Long",StringType(),True), 
    StructField("Actor1Geo_FeatureID",StringType(),True), 
    StructField("Actor2Geo_Type",StringType(),True), 
    StructField("Actor2Geo_FullName",StringType(),True), 
    StructField("Actor2Geo_CountryCode",StringType(),True), 
    StructField("Actor2Geo_ADM1Code",StringType(),True), 
    StructField("Actor2Geo_Lat",StringType(),True), 
    StructField("Actor2Geo_Long",StringType(),True), 
    StructField("Actor2Geo_FeatureID",StringType(),True), 
    StructField("ActionGeo_Type",StringType(),True), 
    StructField("ActionGeo_FullName",StringType(),True), 
    StructField("ActionGeo_CountryCode",StringType(),True), 
    StructField("ActionGeo_ADM1Code",StringType(),True), 
    StructField("ActionGeo_Lat",StringType(),True), 
    StructField("ActionGeo_Long",StringType(),True), 
    StructField("ActionGeo_FeatureID",StringType(),True), 
    StructField("DATEADDED",StringType(),True), 
    StructField("SOURCEURL",StringType(),True)])
dates = [
    20150612, 20150826, 20150126, 20150206, 20141009, 20141027, 20140616,
    20140619, 20140410, 20140428, 20140219, 20140320, 20131204, 20140120,
    20130912, 20131113, 20130529, 20130627, 20130206, 20130502, 20121022,
    20121203, 20120910, 20120926, 20120504, 20120905, 20120302, 20120329]

def summarize(dataset):
    """
    Stats about the dataset
    """
    print("schema: %s" % dataset.schema().json())
    labels = dataset.map(lambda r: r.label)
    print("label average: %f" % labels.mean())
    features = dataset.map(lambda r: r.features)
    summary = Statistics.colStats(features)
    print("features average: %r" % summary.mean())


def preprocess(df):
    """
    Convert GDELT files into a format for DecisionTree
    """
    event_codes = udf(lambda x: "1"+x, StringType())
    # test = df.select(event_codes(df.EventCode).alias('EventCode1'))
    df = df.withColumn("EventCode1", event_codes(df.EventCode))
    df = df.withColumn("EventBaseCode1", event_codes(df.EventBaseCode))
    df = df.withColumn("EventRootCode1", event_codes(df.EventRootCode))
    df = df.drop("EventCode").drop("EventBaseCode").drop("EventRootCode")
    df = df.withColumnRenamed("EventCode1", "EventCode").withColumnRenamed("EventBaseCode1", "EventBaseCode").withColumnRenamed("EventRootCode1", "EventRootCode")
    # add label to df
    df = df.withColumn('Label', df.SQLDATE.isin(dates))
    # remove troublesome column
    df = df.filter("EventRootCode != '1--'").cache()
    df = df.filter(((df.Actor1CountryCode == 'CHN') | (df.Actor2CountryCode == 'CHN')) & ((df.Actor1Type1Code == 'BUS') | (df.Actor2Type1Code == 'BUS'))).cache()
    #only use these columns for features
    #dataset = df.select("Label", "IsRootEvent", "EventCode", "EventBaseCode", "EventRootCode", "QuadClass", "GoldsteinScale", "NumMentions", "NumSources", "NumArticles", "AvgTone").cache()
    # NumArticles was actually hurting accuracy
    dataset = df.select("Label", "IsRootEvent", "EventCode",  "AvgTone").cache()
    # create features in format
    data = dataset.map(lambda row: LabeledPoint(float(row[0] is True), Vectors.dense(row[1:]))).toDF()
    data.cache()
    return data

if __name__ == "__main__":
    conf = (SparkConf().setAppName("GdeltRandomForest"))
    sc = SparkContext(conf=conf)

    dataPath = "s3n://gdelt-em/data_test/*"
    df = sqlContext.read.format("com.databricks.spark.csv").options(header="true", delimiter="\t").load(dataPath, schema=eventsSchema)

    data = preprocess(df)

    # Index labels, adding metadata to the label column.
    # Fit on whole dataset to include all labels in index.
    labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(data)
    # Automatically identify categorical features, and index them.
    # We specify maxCategories so features with > 4 distinct values are treated as continuous.
    featureIndexer =\
        VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=300).fit(data)

    # Split the data into training and test sets (30% held out for testing)
    (trainingData, testData) = data.randomSplit([0.7, 0.3])

    # Train a RandomForest model.
    rf = RandomForestClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures", maxBins=300, featureSubsetStrategy="auto", seed=12345)

    # Chain indexers and tree in a Pipeline
    pipeline = Pipeline(stages=[labelIndexer, featureIndexer, rf])

    # Train model.  This also runs the indexers.
    model = pipeline.fit(trainingData)

    # Make predictions.
    predictions = model.transform(testData)

    # Select example rows to display.
    predictions.select("prediction", "indexedLabel", "features").show(5)

    predictionAndLabels = predictions.select("prediction", "indexedLabel").rdd
    metrics = MulticlassMetrics(predictionAndLabels)
    print metrics.confusionMatrix().toArray()
    print "False Positive of Label 0 %f " % metrics.falsePositiveRate(0.0)
    print "Precision %f " % metrics.precision()
    print "Weighted False Positive %f" % metrics.weightedFalsePositiveRate
    print "Weighted Precision %f" % metrics.weightedPrecision
    print "Weighted FScore %f" % metrics.weightedFMeasure()

    # Select (prediction, true label) and compute test error
    #evaluator = MulticlassClassificationEvaluator(labelCol="indexedLabel", predictionCol="prediction", metricName="f1")
    
    #accuracy = evaluator.evaluate(predictions)

    #print "Test Error = %g" % (1.0 - accuracy)

    treeModel = model.stages[2]
    print treeModel  # summary only
