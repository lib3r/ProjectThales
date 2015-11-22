from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import udf

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer, OneHotEncoder, VectorAssembler
from pyspark.mllib.evaluation import MulticlassMetrics
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
    StructField("IsRootEvent",IntegerType(),True), 
    StructField("EventCode",StringType(),True), 
    StructField("EventBaseCode",StringType(),True), 
    StructField("EventRootCode",StringType(),True), 
    StructField("QuadClass",IntegerType(),True), 
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

dates1 = [
    20150612, 20150826, 20150126, 20150206, 20141009, 20141027, 20140616, 
    20140619, 20140410, 20140428, 20140219, 20140320, 20131204, 20140120, 
    20130912, 20131113, 20130529, 20130627, 20130206, 20130502, 20121022, 
    20121203, 20120910, 20120926, 20120504, 20120905, 20120302, 20120329] 

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

def fix_events(df, column_name, column):
    """
    Appends "1" in front of event columns
    """
    df = df.withColumn("temp", regexp_replace(column_name, "^0", "3"))
    df = df.drop(column_name)
    df = df.withColumnRenamed("temp", column_name)
    return df

def events(df,column_name):
    i = column_name+"I"
    v = column_name+"V"
    stringIndexer = StringIndexer(inputCol=column_name, outputCol=i)
    model = stringIndexer.fit(df)
    indexed = model.transform(df)
    encoder = OneHotEncoder(inputCol=i, outputCol=v)
    encoded = encoder.transform(indexed)
    return encoded

def label(sqldate):
    if sqldate in maxima: return 1.0
    elif sqldate in minima: return -1.0
    else: return 0.0

def event_pipeline(dataset):
    EventCodeI = StringIndexer(inputCol="EventCode", outputCol="EventCodeI")
    EventCodeV = OneHotEncoder(dropLast=True, inputCol="EventCodeI", outputCol="EventCodeV")
    EventRootCodeI = StringIndexer(inputCol="EventRootCode", outputCol="EventRootCodeI")
    EventRootCodeV = OneHotEncoder(dropLast=True, inputCol="EventRootCodeI", outputCol="EventRootCodeV")
    EventBaseCodeI = StringIndexer(inputCol="EventBaseCode", outputCol="EventBaseCodeI")
    EventBaseCodeV = OneHotEncoder(dropLast=True, inputCol="EventBaseCodeI", outputCol="EventBaseCodeV")
    assembler = VectorAssembler(inputCols=["IsRootEvent", "EventCodeV", "EventBaseCodeV", "EventRootCodeV", "QuadClass", "GoldsteinScale", "NumMentions", "NumSources", "NumArticles", "AvgTone"], outputCol="features")
    pipeline = Pipeline(stages=[EventCodeI, EventCodeV, EventRootCodeI, EventRootCodeV, EventBaseCodeI, EventBaseCodeV, assembler])
    model = pipeline.fit(dataset)
    output = model.transform(dataset)
    data = output.map(lambda row: LabeledPoint(row[0], row[-1])).toDF().cache()
    return data

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
    '''
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
    dataset = df.select("Label", "IsRootEvent", "EventCode", "AvgTone").cache()
    # create features in format
    data = dataset.map(lambda row: LabeledPoint(float(row[0] is True), Vectors.dense(row[1:]))).toDF()
    data.cache()
    return data
    '''

    # remove troublesome rows
    df = df.filter("EventRootCode != '--'")
    df = df.filter("EventRootCode != 'X'")
    # add label to df
    labeler = udf(label, FloatType())
    df = df.withColumn('Label', labeler(df.SQLDATE))
    #only use these columns for features
    dataset = df.select("Label", "IsRootEvent", "EventCode", "EventBaseCode", "EventRootCode", "QuadClass", "GoldsteinScale", "NumMentions", "NumSources", "NumArticles", "AvgTone")
    #dataset = df.select("Label", "IsRootEvent", "EventCode", "AvgTone")
    data = event_pipeline(dataset)
    # create features in format
    # data = dataset.map(lambda row: LabeledPoint(row[0], Vectors.dense(row[1:]))).toDF()
    # data.cache()
    return data

def evaluate(predictions):
    """
    Evaluation Metrics
    """
    # label to indexedLabel mappings
    # out = sorted(set([(i[0], i[1]) for i in predictions.select(predictions.label, predictions.indexedLabel).collect()]), key=lambda x: x[0])
    print "Predictions"
    predictions.select("prediction", "indexedLabel", "features").show(5)
    # Select (prediction, true label) and evaluate model
    predictionAndLabels = predictions.select("prediction", "indexedLabel").rdd
    metrics = MulticlassMetrics(predictionAndLabels)
    # Overall statistics
    precision = metrics.precision()
    recall = metrics.recall()
    f1Score = metrics.fMeasure()
    print("Summary Stats")
    print("Precision = %s" % precision)
    print("Recall = %s" % recall)
    print("F1 Score = %s" % f1Score)
    # Statistics by class
    labels = predictions.map(lambda lp: lp.label).distinct().collect()
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
    treeModel = model.stages[2]
    print treeModel # summary only

if __name__ == "__main__":
    sc = SparkContext(appName = "GdeltRF")
    sqlContext = SQLContext(sc)

    dataPath = "s3n://gdelt-em/data_test/*"
    df = sqlContext.read.format("com.databricks.spark.csv").options(header="true", delimiter="\t").load(dataPath, schema=eventsSchema)

    data = preprocess(df)

    # Index labels, adding metadata to the label column.
    # Fit on whole dataset to include all labels in index.
    labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(data)
    # Automatically identify categorical features, and index them.
    # We specify maxCategories so features with > 4 distinct values are treated as continuous.
    featureIndexer =\
        VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=310).fit(data)

    # Split the data into training and test sets (30% held out for testing)
    (trainingData, testData) = data.randomSplit([0.7, 0.3])

    # Train a RandomForest model.
    rf = RandomForestClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures", numTrees=1, maxDepth=30, maxBins=500, featureSubsetStrategy="auto", seed=12345)

    # Chain indexers and tree in a Pipeline
    pipeline = Pipeline(stages=[labelIndexer, featureIndexer, rf])

    # Train model.  This also runs the indexers.
    model = pipeline.fit(trainingData)

    # Make predictions.
    predictions = model.transform(testData)

    evaluate(predictions)
    sc.stop()

    '''
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
    '''
