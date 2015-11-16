import re

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import udf
from pyspark.sql import Row

from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.evaluation import MulticlassMetrics

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

maxima = [
    20150817, 20150723, 20150612, 20150427, 20150126, 20150107, 20141009,
    20140819, 20140616, 20140410, 20140219, 20131204, 20131014, 20130711, 
    20130529, 20130419, 20130206, 20121022, 20120910, 20120809, 20120504, 
    20120302]

minima = [
    20150826, 20150803, 20150708, 20150507, 20150206, 20150119, 20141027, 
    20140828, 20140619, 20140519, 20140320, 20140120, 20131113, 20130729, 
    20130627, 20130502, 20130415, 20121203, 20120926, 20120905, 20120731, 
    20120329]

dates = [
    20150817, 20150723, 20150612, 20150427, 20150126, 20150107, 20141009, 
    20140819, 20140616, 20140410, 20140219, 20131204, 20131014, 20130711, 
    20130529, 20130419, 20130206, 20121022, 20120910, 20120809, 20120504, 
    20120302, 20150826, 20150803, 20150708, 20150507, 20150206, 20150119, 
    20141027, 20140828, 20140619, 20140519, 20140320, 20140120, 20131113, 
    20130729, 20130627, 20130502, 20130415, 20121203, 20120926, 20120905, 
    20120731, 20120329]

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

def fix_events(df, column_name, column):
    """ 
    Appends "1" in front of event columns
    """

    df = df.withColumn("temp", regexp_replace(column_name,"^","**"))
    df = df.drop(column_name)
    df = df.withColumnRenamed("temp",column_name)
    return df

def label(sqldate):
    if sqldate in maxima: return 1.0
    elif sqldate in minima: return 2.0
    else: return 0.0



def preprocess(df):
    """ 
    Convert GDELT files into a format for DecisionTree
    """

    # remove troublesome rows
    df = df.filter("EventRootCode != '--'")
    df = df.filter("EventRootCode != 'X'")


    # fix event columns
    df = fix_events(df,"EventCode",df.EventCode)
    df = fix_events(df,"EventBaseCode",df.EventBaseCode)
    df = fix_events(df,"EventRootCode",df.EventRootCode).cache()


    # add label to df
    labeler = udf(label, FloatType())
    df = df.withColumn('Label',labeler(df.SQLDATE))
    # df = df.withColumn('Label',df.SQLDATE.isin(dates))

    print "Label Counts:"
    df.select("Label").groupby("Label").count().show()

    #only use these columns for features
    dataset = df.select("Label","IsRootEvent", "EventCode", "EventBaseCode","EventRootCode", "QuadClass","GoldsteinScale","NumMentions","NumSources","NumArticles","AvgTone").cache()

    # create features in format
    data = dataset.map(lambda row: LabeledPoint(row[0], row[1:]))
    data.cache()

    return data

def evaluate(labelsAndPredictions, data):
    """
    Evaluation Metrics
    """

    testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(testData.count())
    print('Accuracy = ' + str(1 - testErr))

    # Instantiate metrics object
    metrics = MulticlassMetrics(labelsAndPredictions)

    # Overall statistics
    precision = metrics.precision()
    recall = metrics.recall()
    f1Score = metrics.fMeasure()
    print("Summary Stats")
    print("Precision = %s" % precision)
    print("Recall = %s" % recall)
    print("F1 Score = %s" % f1Score)

    # Statistics by class
    labels = data.map(lambda lp: lp.label).distinct().collect()
    for label in sorted(labels):
        print("Class %s precision = %s" % (label, metrics.precision(label)))
        print("Class %s recall = %s" % (label, metrics.recall(label)))
        print("Class %s F1 Measure = %s" % (label, metrics.fMeasure(label, beta=1.0)))

    # Weighted stats
    print("Weighted recall = %s" % metrics.weightedRecall)
    print("Weighted precision = %s" % metrics.weightedPrecision)
    print("Weighted F(1) Score = %s" % metrics.weightedFMeasure())
    print("Weighted F(0.5) Score = %s" % metrics.weightedFMeasure(beta=0.5))
    print("Weighted false positive rate = %s" % metrics.weightedFalsePositiveRate)


if __name__ == "__main__":

    sc = SparkContext(appName = "GdeltDT-MLlib")
    sqlContext = SQLContext(sc)
        
    dataPath = "s3n://gdelt-em/data_test/*"
    df = sqlContext.read.format("com.databricks.spark.csv").options(header = "true", delimiter="\t").load(dataPath, schema = eventsSchema).repartition(200)

    data = preprocess(df)
    
    categoricalFeaturesInfo = {
        1: df.select("IsRootEvent").distinct().count(), 
        2: df.select("EventCode").distinct().count(),
        3: df.select("EventBaseCode").distinct().count(),
        4: df.select("EventRootCode").distinct().count(),
        5: df.select("QuadClass").distinct().count()}


    (trainingData, testData) = data.randomSplit([0.7, 0.3])
    # Train a DecisionTree model.
    #  Empty categoricalFeaturesInfo indicates all features are continuous.
    model = DecisionTree.trainClassifier(trainingData, numClasses=3, categoricalFeaturesInfo=categoricalFeaturesInfo,
                                         impurity='gini', maxDepth=5, maxBins=300)

    # Evaluate model on test instances and compute test error
    predictions = model.predict(testData.map(lambda x: x.features))
    labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)

    print('Learned classification tree model:')
    print(model.toDebugString())

    evaluate(labelsAndPredictions, data)

    sc.stop()


