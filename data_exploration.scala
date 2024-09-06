// Databricks notebook source
// MAGIC %md
// MAGIC # Data Exploration
// MAGIC 
// MAGIC In this document I am investigating the following:
// MAGIC 
// MAGIC **City Investigation**
// MAGIC 1. Why are there only 5 cities?
// MAGIC     1. Is any city related to a specific generation method?
// MAGIC     2. Is it possible that these are where the Renewable energies are generated?
// MAGIC     3. Or any other energy for that matter?
// MAGIC     
// MAGIC **Feature investigation**
// MAGIC 2. Link the two datasets and do a regression on each kind of energy with all the weather features.
// MAGIC     1. Check which are correlated. Do feature/variable selection. Build the regressions/models on the appropriate features...
// MAGIC     2. Maybe even use that as a tool to see if there is data drift in the data?

// COMMAND ----------

import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame}
import spark.implicits._
import org.apache.spark.sql.expressions.Window

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Loading the files

// COMMAND ----------

// Reading in files
val ener_schema = StructType(Array(StructField("time",StringType,true),StructField("generation biomass",DoubleType,true),
                                   StructField("generation fossil brown coal/lignite",DoubleType,true),
                                   StructField("generation fossil coal-derived gas",DoubleType,true),
                                   StructField("generation fossil gas",DoubleType,true),StructField("generation fossil hard coal",DoubleType,true),
                                   StructField("generation fossil oil",DoubleType,true),StructField("generation fossil oil shale",DoubleType,true),
                                   StructField("generation fossil peat",DoubleType,true),StructField("generation geothermal",DoubleType,true),
                                   StructField("generation hydro pumped storage aggregated",DoubleType,true),
                                   StructField("generation hydro pumped storage consumption",DoubleType,true),
                                   StructField("generation hydro run-of-river and poundage",DoubleType,true),
                                   StructField("generation hydro water reservoir",DoubleType,true),
                                   StructField("generation marine",DoubleType,true),StructField("generation nuclear",DoubleType,true),
                                   StructField("generation other",DoubleType,true),StructField("generation other renewable",DoubleType,true),
                                   StructField("generation solar",DoubleType,true),StructField("generation waste",DoubleType,true),
                                   StructField("generation wind offshore",DoubleType,true),StructField("generation wind onshore",DoubleType,true),
                                   StructField("forecast solar day ahead",DoubleType,true),StructField("forecast wind offshore eday ahead",DoubleType,true),
                                   StructField("forecast wind onshore day ahead",DoubleType,true),StructField("total load forecast",DoubleType,true),
                                   StructField("total load actual",DoubleType,true),StructField("price day ahead",DoubleType,true),
                                   StructField("price actual",DoubleType,true)))
val weat_schema = StructType(Array(StructField("dt_iso",StringType,true),StructField("city_name",StringType,true),StructField("temp",DoubleType,true),
                                   StructField("temp_min",DoubleType,true),StructField("temp_max",DoubleType,true),StructField("pressure",DoubleType,true),
                                   StructField("humidity",DoubleType,true),StructField("wind_speed",DoubleType,true),StructField("wind_deg",DoubleType,true),
                                   StructField("rain_1h",DoubleType,true),StructField("rain_3h",DoubleType,true),StructField("snow_3h",DoubleType,true),
                                   StructField("clouds_all",DoubleType,true),StructField("weather_id",DoubleType,true),
                                   StructField("weather_main",StringType,true),StructField("weather_description",StringType,true),
                                   StructField("weather_icon",StringType,true)))

val ener_all_df = spark.read.option("header","true").schema(ener_schema).csv("/FileStore/tables/energy_dataset.csv")
val weat_orig_df = spark.read.option("header","true").schema(weat_schema).csv("/FileStore/tables/weather_features.csv")
    .withColumn("city_name",trim('city_name))

// COMMAND ----------

// Reading in applicable energy df
val ener_cols = Seq("time","generation fossil brown coal/lignite","generation fossil gas",
               "generation fossil hard coal","generation fossil oil",
               "generation hydro pumped storage consumption","generation hydro run-of-river and poundage",
               "generation hydro water reservoir","generation biomass","generation other renewable",
               "generation solar","generation waste","generation wind onshore","generation nuclear",
               "generation other","forecast solar day ahead","forecast wind onshore day ahead",
               "total load forecast","total load actual","price day ahead",
               "price actual")

val ener_class_cols = Seq("fossil","hydro","nuclear","solar","wind","waste","renewable")

val ener_df = ener_all_df.select(ener_cols.map(col):_*)
    .withColumn("gen_fossil_totals",$"generation fossil brown coal/lignite" + $"generation fossil gas" + $"generation fossil hard coal" + $"generation fossil oil")
    .withColumn("gen_nuclear_totals", $"generation nuclear")
    .withColumn("gen_hydro_totals", $"generation hydro pumped storage consumption" + $"generation hydro run-of-river and poundage" + $"generation hydro water reservoir")
    .withColumn("gen_solar_totals", $"generation solar")
    .withColumn("gen_wind_totals", $"generation wind onshore")
    .withColumn("gen_waste_totals", $"generation biomass" + $"generation waste")
    .withColumn("gen_renewable_totals", $"generation other renewable" + 'gen_hydro_totals + 'gen_solar_totals + 'gen_wind_totals + 'gen_waste_totals)

ener_df.count

// COMMAND ----------

val cities = weat_orig_df.select("city_name").distinct.take(20).map(_.toString.replace(" ","").replace("[","").replace("]",""))
val init_df: DataFrame = weat_orig_df.select("dt_iso").distinct
val weat_df = cities.foldLeft(init_df)((df,city) => {
  val stg_df = weat_orig_df.filter('city_name === city).drop('city_name)
  val city_cols = weat_orig_df.columns.slice(2,weat_orig_df.columns.size).toSeq.map(c => c + "_" + city).toSeq
  val stg_cols = Seq("dt_iso") ++ city_cols
  val stg_fin_df = stg_df.toDF(stg_cols: _*)
  df.join(stg_fin_df,Seq("dt_iso"),"left_outer")
})
  .cache
weat_df.count

// COMMAND ----------

// Checking the size of each df for each city
cities.foreach(city => {
  val df_count = weat_orig_df.filter('city_name === city).count
  println(f"${city} count: ${df_count}")
})

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Exploring weather information

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC #### Checking weather IDs and weather descriptions

// COMMAND ----------

weat_orig_df
  .select('city_name,'weather_main,'weather_ID,'weather_description,'weather_icon)
  .groupBy('weather_ID,'weather_main,'weather_description)
  .count
  .orderBy('weather_ID)
  .show(300,false)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC From the above these are the relations:
// MAGIC 1. weather_ID:weather_main --> 1:many
// MAGIC 2. weather_ID:weather_description --> 1:1 (...mostly) - there are some exceptions, where the word 'proximity' features in these 'duplicate' descriptions. There is only one exception from this rule namely 612, and "light shower sleet" is chosen between the two descriptions.
// MAGIC 3. weather_ID:weather_icon --> many:2 - 
// MAGIC 4. weather_main:weather_icon --> 1:2 - there is a 'day' and 'night' icon for each main weather attribute makes sense... (and for this reason was not shown in the above output)
// MAGIC 
// MAGIC From the above, it is clear that I can now create a dictionary for the weather discriptions. I will then use this dictionary for the imputation.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC #### Creating weather Dictionary

// COMMAND ----------

val init_weat_dict = weat_orig_df
  .select('city_name,'weather_main,'weather_ID,'weather_description,'weather_icon)
  .groupBy('weather_ID,'weather_main,'weather_description,'weather_icon)
  .count
  .orderBy('weather_ID)

val my_replace = udf((data: String , str_old : String, str_new: String) => data.replaceAll(str_old,str_new).trim())

val day_night = Seq("01d","01n","02d","02n","03d","03n","04d","04n","09d","09n","10d","10n","11d","11n","13d","13n","50d","50n")

// init_weat_dict.groupBy('weather_icon).count.orderBy('weather_icon).show(100,false)

val weat_dict = init_weat_dict
  .withColumn("weather_description",my_replace('weather_description,lit("proximity"),lit("")))
  .withColumn("weather_description",my_replace('weather_description,lit("shower sleet"),lit(" light shower sleet")))
  .withColumn("weather_description",my_replace('weather_description,lit("light light shower sleet"),lit("light shower sleet")))
  .withColumn("weather_icon", when('weather_icon.isInCollection(day_night),'weather_icon).otherwise(expr("trim(concat(weather_icon,'d'))")))
  .groupBy('weather_ID,'weather_main,'weather_description,'weather_icon)
  .count()
  .cache

weat_dict.show(100,false)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC The dataset begins at 2015-01-01 and ends at 2018-12-31. This is a total of 1461 days, and equates to 35064 hours over this period. THerefore there are some days that are duplicated over this time.
// MAGIC 
// MAGIC Two questions:
// MAGIC 1. Which days..?
// MAGIC 2. Why..?

// COMMAND ----------

// The below gives a summary of which cities have duplicate dates.
// All cities have duplicate days
weat_orig_df.groupBy('dt_iso,'city_name).count.groupBy('city_name,'count).count.orderBy('city_name,desc("count")).show(40)

// COMMAND ----------

weat_orig_df.filter(('dt_iso === "2016-04-16 10:00:00+02:00") && ('city_name === "Seville")).drop('city_name).drop('dt_iso).show()


// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC #### Creating city-level and overall duplicate date dfs

// COMMAND ----------

val mad_dupl_date_df = weat_orig_df.filter('city_name === "Madrid").groupBy('dt_iso).count.withColumnRenamed("count", "counts_mad").filter("counts_mad >= 2").cache
val sev_dupl_date_df = weat_orig_df.filter('city_name === "Seville").groupBy('dt_iso).count.withColumnRenamed("count", "counts_sev").filter("counts_sev >= 2").cache
val bil_dupl_date_df = weat_orig_df.filter('city_name === "Bilbao").groupBy('dt_iso).count.withColumnRenamed("count", "counts_bil").filter("counts_bil >= 2").cache
val val_dupl_date_df = weat_orig_df.filter('city_name === "Valencia").groupBy('dt_iso).count.withColumnRenamed("count", "counts_val").filter("counts_val >= 2").cache
val bar_dupl_date_df = weat_orig_df.filter('city_name === "Barcelona").groupBy('dt_iso).count.withColumnRenamed("count", "counts_bar").filter("counts_bar >= 2").cache
// madrid_dupl_date_df.show()

// COMMAND ----------

val dupl_date_df = mad_dupl_date_df
  .join(sev_dupl_date_df,Seq("dt_iso"),"outer")
  .join(bil_dupl_date_df,Seq("dt_iso"),"outer")
  .join(val_dupl_date_df,Seq("dt_iso"),"outer")
  .join(bar_dupl_date_df,Seq("dt_iso"),"outer")
  .na.fill(0)
  .withColumn("all_dupl",('counts_mad > 0) && ('counts_sev > 0) && ('counts_bil > 0) && ('counts_bar >0))
  .orderBy(desc("all_dupl"))
  .cache

println(f"Duplicate date count is: ${dupl_date_df.count} [${dupl_date_df.count/35064.0*100}%2.2f%%]")
dupl_date_df.show(5,false)

// COMMAND ----------

weat_orig_df
  .filter("trim(lower(city_name)) = 'valencia'")
  .join(dupl_date_df,Seq("dt_iso"),"inner")
  .groupBy('weather_main)
  .count
  .orderBy('weather_main)
  .show(100,false)

// COMMAND ----------

// MAGIC %md
// MAGIC # Imputing the correct weather values
// MAGIC 
// MAGIC Here I will be imputing the correct weather descriptions given the weather attributes. I will be testing different classifiers for this, and also only do this per city, as my assumption is that the different cities will have different overall weather descriptions for the same weather attributes (might be wrong, but it is more efficient doing it this way from the get-go)

// COMMAND ----------

// DBTITLE 1,ML imports
// My own version of pipelines -- Setting up Libraries for ML
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.{Interaction,VectorAssembler,Normalizer,PCA}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

// Building the pipeline -- setting up pipelines
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{LogisticRegression, DecisionTreeClassifier,
                                           MultilayerPerceptronClassifier, RandomForestClassifier}
import org.apache.spark.ml.feature.{HashingTF, Tokenizer, VectorIndexer, StringIndexer, IndexToString}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.Row

// Evaluation
import org.apache.spark.mllib.evaluation.MultilabelMetrics
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Pipeline

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Building pipeline

// COMMAND ----------

// MAGIC %md
// MAGIC ### Creating balanced sampler

// COMMAND ----------

def balancedSampler(df: DataFrame, basedOn_col: String, minRowsPerCategory: Int=100): DataFrame = {
  val df_cols = df.columns
  assert(df_cols.contains(basedOn_col),"basedOn_col not present in df.columns")
  val col_vals = df.select(basedOn_col).distinct.take(20).map(_.toString.replace(" ","").replace("[","").replace("]",""))
  // col_vals.foreach(println)
  val window = Window.partitionBy(basedOn_col).orderBy(desc("unif"))
  
  val sample_df = df.withColumn("unif",rand)

  sample_df
      .withColumn("cat_rank",dense_rank.over(window))
      .filter('cat_rank <= minRowsPerCategory)
}


// COMMAND ----------

// MAGIC %md
// MAGIC ### Testing the pipeline model

// COMMAND ----------

// Example from: https://spark.apache.org/docs/2.4.5/ml-pipeline.html

// Below are the important columns to consider per city
val dedup_excl_cols = dupl_date_df.columns.toSeq.filter(!_.contains("dt_iso"))
val input_cols = Seq("temp","temp_min","temp_max","pressure","humidity","wind_speed","wind_deg","rain_1h","rain_3h","snow_3h","clouds_all")
val label_col = "weather_main"


// Creating train data excluding duplicate times
val city = "seville"
val train_data = weat_orig_df
  .filter(f"lower(trim(city_name)) = '${city}'")
  .join(dupl_date_df,Seq("dt_iso"),"left")
  .filter('all_dupl.isNull)
  .drop(dedup_excl_cols:_*)
  .cache

// Creating training and testing set (NOT FILTERED)
val Array(training_full, testing) = train_data.withColumn("label_preIndex",col(label_col)).randomSplit(Array(0.6, 0.4)).map(_.cache)
val training = balancedSampler(df=training_full,basedOn_col="weather_main",minRowsPerCategory=150).cache
val k_classes = training_full.select(label_col).distinct.count.toInt

// Filtering data
val train_df = training
  .cache
val test_df = testing
  .cache

// // Creating pipeline stages -- transformers
val assembler1 = new VectorAssembler()
    .setInputCols(input_cols.toArray)
    .setOutputCol("features")
val label_indexer = new StringIndexer()
  .setInputCol("label_preIndex")
  .setOutputCol("label")
  .fit(training_full)
val label_ind_converter = new IndexToString()
  .setInputCol("prediction")
  .setOutputCol("predictedLabel")
  .setLabels(label_indexer.labels)


// // Estimators considered
val lr = new LogisticRegression()
  .setMaxIter(10)
  .setRegParam(0.001)
val layers = Array(input_cols.size.toInt,10,8,10,k_classes)
val nn = new MultilayerPerceptronClassifier()
  .setLayers(layers)
  .setBlockSize(128)
  .setSeed(1234L)
  .setMaxIter(100)
  .setPredictionCol("prediction")

// Creating pipeline stages -- estimators
val rf = new RandomForestClassifier()
  .setLabelCol("label")
  .setFeaturesCol("features")
  .setPredictionCol("prediction")
val dt = new DecisionTreeClassifier()
  .setLabelCol("label")
  .setFeaturesCol("features")
  .setPredictionCol("prediction")

// Compiling pipeline
val pipeline = new Pipeline()
  .setStages(Array(assembler1, label_indexer, dt, label_ind_converter))

println(f"train_df count: ${train_df.count}")
println(f"test_df count: ${test_df.count}")

// Fit the pipeline to training documents.
val pipe_model = pipeline.fit(train_df)
val pred_df = pipe_model.transform(test_df).cache


// COMMAND ----------

println(f"The testing df count: ${pred_df.count}")
pred_df
  .select('dt_iso,'weather_id,'weather_main,'label,'probability,'prediction)
  .withColumn("pred_lable_diff",when('prediction - 'label > 0,"pos_diff").when('prediction - 'label < 0,"neg_diff").otherwise("same"))
  .groupBy('pred_lable_diff,'weather_main)
  .count
  .withColumnRenamed("count",city)
  .orderBy('pred_lable_diff,'weather_main)
  .show(100)

// COMMAND ----------

// Example from: https://spark.apache.org/docs/2.4.5/mllib-evaluation-metrics.html#multilabel-classification

// Instantiate metrics object
val metrics = new MultilabelMetrics(pred_df.select('label,'prediction).rdd.map(r => (Array(r.getDouble(0)),Array(r.getDouble(1)))))

// Summary stats
println(s"Recall_${city} = ${metrics.recall}")
println(s"Precision_${city} = ${metrics.precision}")
println(s"F1_${city} = ${metrics.f1Measure}")
println(s"Accuracy_${city} = ${metrics.accuracy}")

// Individual label stats
metrics.labels.foreach(label =>
  println(s"Class $label precision = ${metrics.precision(label)}"))
metrics.labels.foreach(label => println(s"Class $label recall = ${metrics.recall(label)}"))
metrics.labels.foreach(label => println(s"Class $label F1-score = ${metrics.f1Measure(label)}"))

// Micro stats
println(s"Micro_recall_${city} = ${metrics.microRecall}")
println(s"Micro_precision_${city} = ${metrics.microPrecision}")
println(s"Micro_F1_${city} = ${metrics.microF1Measure}")

// Hamming loss
println(s"Hamming_loss_${city} = ${metrics.hammingLoss}")

// Subset accuracy
println(s"Subset_accuracy_${city} = ${metrics.subsetAccuracy}")

// COMMAND ----------

pred_df.groupBy('prediction,'predictedLabel).count.withColumnRenamed("count",city).orderBy('prediction).show(40,false)

// COMMAND ----------

// MAGIC %md
// MAGIC From the above analysis the following has been concluded:
// MAGIC 1. RandomForrest is the best performing classifier
// MAGIC 2. You need to balance your training set before you predict
// MAGIC 3. In all algorithms when applying PCA the accuracy decreased (understandable)
// MAGIC 4. In all algorithms when normalizing the data the accuracy decreased (not clear why understandable)
// MAGIC 5. Each city needs to be predicted on its own
// MAGIC 
// MAGIC In this analysis was tested: NeuralNet, LogisticRegression, DecisionTree RandomForrest. NaiveBayse needs non-negative values, and SVM is implemented as a binaryclassifier, which is not appropriate for our use case.
// MAGIC 
// MAGIC Now I will predict the duplicate weather_main for each duplicate time. I will see if the predicted weather_main is in the list of duplicate weather_main's, and if so that is the simple choice. There are three scenario's to cater for:
// MAGIC 1. Predicted is part of the list, and the list is unique, that weather_main is chosen (column == sc1_select)
// MAGIC 2. If there is duplicate weather_mains, I will choose the min(weather_id) (column == sc2_select)
// MAGIC 3. If it is not in the set, then I will disregard the prediction and chose from the list based on weather_id difference (column == sc3_select)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Impute values

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ###Prediction of duplicate times function

// COMMAND ----------

// Example from: https://spark.apache.org/docs/2.4.5/ml-pipeline.html

def predictDuplicateTimesWeatherMain(df: DataFrame, city: String): DataFrame = {
  // setting up columns
  val dedup_excl_cols = dupl_date_df.columns.toSeq.filter(!_.contains("dt_iso"))
  val input_cols = Seq("temp","temp_min","temp_max","pressure","humidity","wind_speed","wind_deg","rain_1h","rain_3h","snow_3h","clouds_all")
  val label_col = "weather_main"
  val filter_dup_count_cols_map = Map(
    "madrid" -> "counts_mad",
    "seville" -> "counts_sev",
    "bilbao" -> "counts_bil",
    "valencia" -> "counts_val",
    "barcelona" -> "counts_bar")


  // Creating train data excluding duplicate times
  val train_data = df
    .filter(f"lower(trim(city_name)) = '${city}'")
    .join(dupl_date_df,Seq("dt_iso"),"left")
    .filter('all_dupl.isNull)
    .withColumn("label_preIndex",col(label_col))
    .drop(dedup_excl_cols:_*)
    .cache

  val duplice_data = df
    .filter(f"lower(trim(city_name)) = '${city}'")
    .join(dupl_date_df,Seq("dt_iso"),"left")
//     .filter('all_dupl.isNotNull)
    .na.fill(0)
    .filter(f"${filter_dup_count_cols_map(city)} > 0")
    .drop(dedup_excl_cols:_*)
    .cache

  // // Creating training and testing set
  val training = balancedSampler(
      df=train_data,
      basedOn_col="weather_main",
      minRowsPerCategory=150)
    .cache
  val k_classes = training.select(label_col).distinct.count.toInt

  // // Filtering data
  val train_df = training.cache
  val test_df = duplice_data.cache
  // train_df.groupBy('weather_main).count.orderBy('weather_main).show(40,false)

  println(f"${city} train_df count: ${train_df.count}")
  println(f"${city} test_df count: ${test_df.count}")
//   train_df.groupBy('weather_main).count.orderBy('weather_main).show(40,false)

  // // Creating pipeline stages -- transformers
  val assembler1 = new VectorAssembler()
      .setInputCols(input_cols.toArray)
      .setOutputCol("features")
  val label_indexer = new StringIndexer()
    .setInputCol("label_preIndex")
    .setOutputCol("label")
    .fit(train_data)
  val label_ind_converter = new IndexToString()
    .setInputCol("prediction")
    .setOutputCol("predictedLabel")
    .setLabels(label_indexer.labels)

  // // Creating pipeline stages -- estimators
  val rf = new RandomForestClassifier()
    .setLabelCol("label")
    .setFeaturesCol("features")
    .setPredictionCol("prediction")

  // // Compiling pipeline
  val pipeline = new Pipeline()
    .setStages(Array(assembler1, label_indexer, rf, label_ind_converter))

  // Fit the pipeline to training documents.
  val pipe_model = pipeline.fit(train_df)
  val pred_df = pipe_model.transform(test_df).cache
  return pred_df
}



// COMMAND ----------

val pred_df_bar = predictDuplicateTimesWeatherMain(df=weat_orig_df,city="barcelona")
val pred_df_bil = predictDuplicateTimesWeatherMain(df=weat_orig_df,city="bilbao")
val pred_df_mad = predictDuplicateTimesWeatherMain(df=weat_orig_df,city="madrid")
val pred_df_sev = predictDuplicateTimesWeatherMain(df=weat_orig_df,city="seville")
val pred_df_val = predictDuplicateTimesWeatherMain(df=weat_orig_df,city="valencia")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ###Building Selector function

// COMMAND ----------

// DBTITLE 1,Building Selector function

def predictionSelector(df: DataFrame):DataFrame = {
  
  val orig_cols = df.columns.toSeq
  val final_select_cols = orig_cols ++ Seq("weat_main_select")
  
  val sc1_wind = Window.partitionBy('dt_iso,'weather_main)
  val sc2_wind = Window.partitionBy('dt_iso,'weather_main).orderBy('pred_min_id_diff)
  val sc3_wind = Window.partitionBy('dt_iso)

  // weat_dict.groupBy('weather_main).min("weather_id").orderBy('weather_main).show(100,false)
  val weat_minID_map = Map("clear" -> 800,"clouds" -> 801,"drizzle" -> 300,"dust" -> 731,"fog" -> 741,
                           "haze" -> 721,"mist" -> 701,"rain" -> 500,"smoke" -> 711,"snow" -> 600,
                           "squall" -> 771,"thunderstorm" -> 200)
  val weatMinID_col = typedLit(weat_minID_map)

  // 1. Predicted is part of the list, and the list is unique, that weather_main is chosen (column == sc1_select)
  // 2. If there is duplicate weather_mains, I will choose the min(weather_id) (column == sc2_select)
  // 3. If it is not in the set, then I will disregard the prediction and chose from the list based on min(weather_id) (column == sc3_select)

  val ret_df = df
    .withColumn("pred_min_id", coalesce(weatMinID_col($"predictedLabel"), lit("")))
    .withColumn("pred_min_id_diff", 'weather_id - 'pred_min_id)
    .withColumn("sc2_rownum", row_number.over(sc2_wind))
    .withColumn("sc3_min_id", min('weather_id).over(sc3_wind))
    .withColumn("time_weat_main_count", count('weather_main).over(sc2_wind))
    .withColumn("sc1_select", when(('predictedLabel === 'weather_main) && ('time_weat_main_count === 1),1).otherwise(0))
    .withColumn("sc2_select", when(('pred_min_id_diff ===  0) && ('sc2_rownum ===  1) && ('sc1_select === 0),1).otherwise(0))
    .withColumn("s1_s2_sum", sum('sc1_select + 'sc2_select).over(sc3_wind))
    .withColumn("sc3_select", when(('weather_id ===  'sc3_min_id) && ('s1_s2_sum ===  0) && ('sc2_rownum ===  1),1).otherwise(0))
    .withColumn("weat_main_select",'sc1_select + 'sc2_select + 'sc3_select)
  
  return ret_df.select(final_select_cols.map(col):_*)
  
}

val pred_final_df_bar = predictionSelector(pred_df_bar).cache
val pred_final_df_bil = predictionSelector(pred_df_bil).cache
val pred_final_df_mad = predictionSelector(pred_df_mad).cache
val pred_final_df_sev = predictionSelector(pred_df_sev).cache
val pred_final_df_val = predictionSelector(pred_df_val).cache




// COMMAND ----------

// MAGIC %md
// MAGIC ## Testing and saving results

// COMMAND ----------

val mad_time_count4 = Seq("2018-03-14 10:00:00+01:00","2018-11-10 19:00:00+01:00","2018-02-27 17:00:00+01:00","2017-01-27 14:00:00+01:00","2018-01-07 14:00:00+01:00")
val all_dup_times = Seq("2015-10-01 02:00:00+02:00","2016-09-30 02:00:00+02:00","2017-09-30 02:00:00+02:00","2018-09-30 02:00:00+02:00")
val weat_orig_cols = weat_orig_df.columns.toSeq


pred_final_df_bar
  .union(pred_final_df_bil)
  .union(pred_final_df_mad)
  .union(pred_final_df_sev)
  .union(pred_final_df_val)
  .filter('weat_main_select === 1)
  .select(weat_orig_cols.map(col):_*)
  .repartition(1)
  .write.format("com.databricks.spark.csv")
  .option("header", "true")
  .save("/FileStore/tables/finalPredictedWeatherMain_onlyDuplicateTimes.csv")

val pred_schema = pred_final_df_bar.schema





// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Reading the file for further processing

// COMMAND ----------


val pred_select_df = spark.read.format("csv")
      .option("header", "true")
      .schema(weat_schema) // the read file has the same schema as the original files schema; it is defined at the beginning
      .load("/FileStore/tables/finalPredictedWeatherMain_onlyDuplicateTimes.csv")

pred_select_df.count

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Creating final Weather_DF and saving as CSV

// COMMAND ----------

val weat_orig_cols = weat_orig_df.columns.toSeq
val union_wind = Window.partitionBy('dt_iso,'city_name)

val weat_init_df = weat_orig_df
    .select(weat_orig_cols.map(col):_*)
    .withColumn("dataset",lit("original"))
    .union(pred_select_df.select(weat_orig_cols.map(col):_*).withColumn("dataset",lit("predicted")))
    .withColumn("time_city_count",count("city_name").over(union_wind))
    .filter(('time_city_count === 1) || (('time_city_count > 1) && ('dataset === "predicted")))
    .select(weat_orig_cols.map(col):_*)
    .cache

val cities = weat_orig_df.select("city_name").distinct.take(20).map(_.toString.replace(" ","").replace("[","").replace("]",""))

val init_df: DataFrame = weat_orig_df.select("dt_iso").distinct
val weat_df = cities.foldLeft(init_df)((df,city) => {
  val lower_city = city.toLowerCase
  val stg_df = weat_init_df.filter(f"trim(lower(city_name)) = '${lower_city}'").drop('city_name)
  val city_cols = weat_orig_df.columns.slice(2,weat_orig_df.columns.size).toSeq.map(c => c + "_" + city).toSeq
  val stg_cols = Seq("dt_iso") ++ city_cols
  val stg_fin_df = stg_df.toDF(stg_cols: _*)
  df.join(stg_fin_df,Seq("dt_iso"),"left_outer")
})
  
weat_df.repartition(1)
  .write.format("com.databricks.spark.csv")
  .mode(SaveMode.Overwrite)
  .option("header", "true")
  .save("/FileStore/tables/finalWeatherData.csv")


// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Checking union

// COMMAND ----------

println(f"Unique dates: ${init_df.count}")
println(f"weat_init_df count: ${weat_init_df.count}")
println(f"should be: ${init_df.count*5}")

// COMMAND ----------

weat_df.createOrReplaceTempView("weat_table")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select * from weat_table

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Reading WeatherData.csv

// COMMAND ----------

val weather_schema = StructType(Array(StructField("dt_iso",StringType,true),StructField("temp_Madrid",DoubleType,true),
                                      StructField("temp_min_Madrid",DoubleType,true),StructField("temp_max_Madrid",DoubleType,true),
                                      StructField("pressure_Madrid",DoubleType,true),StructField("humidity_Madrid",DoubleType,true),
                                      StructField("wind_speed_Madrid",DoubleType,true),StructField("wind_deg_Madrid",DoubleType,true),
                                      StructField("rain_1h_Madrid",DoubleType,true),StructField("rain_3h_Madrid",DoubleType,true),
                                      StructField("snow_3h_Madrid",DoubleType,true),StructField("clouds_all_Madrid",DoubleType,true),
                                      StructField("weather_id_Madrid",DoubleType,true),StructField("weather_main_Madrid",StringType,true),
                                      StructField("weather_description_Madrid",StringType,true),StructField("weather_icon_Madrid",StringType,true),
                                      StructField("temp_Seville",DoubleType,true),StructField("temp_min_Seville",DoubleType,true),
                                      StructField("temp_max_Seville",DoubleType,true),StructField("pressure_Seville",DoubleType,true),
                                      StructField("humidity_Seville",DoubleType,true),StructField("wind_speed_Seville",DoubleType,true),
                                      StructField("wind_deg_Seville",DoubleType,true),StructField("rain_1h_Seville",DoubleType,true),
                                      StructField("rain_3h_Seville",DoubleType,true),StructField("snow_3h_Seville",DoubleType,true),
                                      StructField("clouds_all_Seville",DoubleType,true),StructField("weather_id_Seville",DoubleType,true),
                                      StructField("weather_main_Seville",StringType,true),StructField("weather_description_Seville",StringType,true),
                                      StructField("weather_icon_Seville",StringType,true),StructField("temp_Bilbao",DoubleType,true),
                                      StructField("temp_min_Bilbao",DoubleType,true),StructField("temp_max_Bilbao",DoubleType,true),
                                      StructField("pressure_Bilbao",DoubleType,true),StructField("humidity_Bilbao",DoubleType,true),
                                      StructField("wind_speed_Bilbao",DoubleType,true),StructField("wind_deg_Bilbao",DoubleType,true),
                                      StructField("rain_1h_Bilbao",DoubleType,true),StructField("rain_3h_Bilbao",DoubleType,true),
                                      StructField("snow_3h_Bilbao",DoubleType,true),StructField("clouds_all_Bilbao",DoubleType,true),
                                      StructField("weather_id_Bilbao",DoubleType,true),StructField("weather_main_Bilbao",StringType,true),
                                      StructField("weather_description_Bilbao",StringType,true),StructField("weather_icon_Bilbao",StringType,true),
                                      StructField("temp_Barcelona",DoubleType,true),StructField("temp_min_Barcelona",DoubleType,true),
                                      StructField("temp_max_Barcelona",DoubleType,true),StructField("pressure_Barcelona",DoubleType,true),
                                      StructField("humidity_Barcelona",DoubleType,true),StructField("wind_speed_Barcelona",DoubleType,true),
                                      StructField("wind_deg_Barcelona",DoubleType,true),StructField("rain_1h_Barcelona",DoubleType,true),
                                      StructField("rain_3h_Barcelona",DoubleType,true),StructField("snow_3h_Barcelona",DoubleType,true),
                                      StructField("clouds_all_Barcelona",DoubleType,true),StructField("weather_id_Barcelona",DoubleType,true),
                                      StructField("weather_main_Barcelona",StringType,true),StructField("weather_description_Barcelona",StringType,true),
                                      StructField("weather_icon_Barcelona",StringType,true),StructField("temp_Valencia",DoubleType,true),
                                      StructField("temp_min_Valencia",DoubleType,true),StructField("temp_max_Valencia",DoubleType,true),
                                      StructField("pressure_Valencia",DoubleType,true),StructField("humidity_Valencia",DoubleType,true),
                                      StructField("wind_speed_Valencia",DoubleType,true),StructField("wind_deg_Valencia",DoubleType,true),
                                      StructField("rain_1h_Valencia",DoubleType,true),StructField("rain_3h_Valencia",DoubleType,true),
                                      StructField("snow_3h_Valencia",DoubleType,true),StructField("clouds_all_Valencia",DoubleType,true),
                                      StructField("weather_id_Valencia",DoubleType,true),StructField("weather_main_Valencia",StringType,true),
                                      StructField("weather_description_Valencia",StringType,true),StructField("weather_icon_Valencia",StringType,true)))
                                
val weather_df = spark.read.format("csv")
      .option("header", "true")
      .schema(weather_schema)
      .load("/FileStore/tables/finalWeatherData.csv")

// COMMAND ----------

import org.apache.spark.ml.feature.Imputer

val joined_df = ener_df.join(weather_df,'time === 'dt_iso,"inner").cache

val new_cols = joined_df.columns.map(c => c.replace(" ","_").replace("/","_").replace("-","-"))

val final_df = joined_df.toDF(new_cols:_*)
  .withColumn("time", from_unixtime(unix_timestamp(col("time"), "yyyy-mm-dd HH:mm")))
  .withColumn("date", to_date('time, "yyyy-mm-dd"))
  .withColumn("date_time", to_timestamp('time, "yyyy-mm-dd HH:mm"))

val fin_col_order = final_df.columns.toSeq
val input_cols = final_df.dtypes.filter(_._2.toString.contains("DoubleType")).map(_._1).toArray
val output_cols = input_cols.map(c => c + "_out")

val imput_est = new Imputer()
  .setInputCols(input_cols)
  .setOutputCols(output_cols)

val imput_model = imput_est.fit(final_df)
val imputted_df = imput_model.transform(final_df)

val final_imput_df = input_cols.foldLeft(imputted_df)((df,col) => {
  val new_col = col + "_out"
  df
  .drop(col)
  .withColumnRenamed(new_col,col)
}).select(fin_col_order.map(col):_*)

final_imput_df.repartition(1)
  .write.format("com.databricks.spark.csv")
  .option("header", "true")
  .mode(SaveMode.Overwrite)
  .save("/FileStore/tables/finalEnergyWeatherData.csv")

// // final_df.schema.foreach(println)


// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Investigating NAs and data checks

// COMMAND ----------

println(f"final_df NA count: ${final_df.count - final_df.na.drop.count}")
println(f"final_imput_df NA count: ${final_imput_df.count-final_imput_df.na.drop.count}")
println(f"weather_df NA count: ${weather_df.count-weather_df.na.drop.count}")


// COMMAND ----------

val energy_weather_schema = StructType(Array(StructField("time",StringType,true),StructField("generation_fossil_brown_coal_lignite",DoubleType,true),
                                  StructField("generation_fossil_gas",DoubleType,true),StructField("generation_fossil_hard_coal",DoubleType,true),
                                  StructField("generation_fossil_oil",DoubleType,true),StructField("generation_hydro_pumped_storage_consumption",DoubleType,true),
                                              StructField("generation_hydro_run_of_river_and_poundage",DoubleType,true),
                                             StructField("generation_hydro_water_reservoir",DoubleType,true),
                                  StructField("generation_biomass",DoubleType,true),StructField("generation_other_renewable",DoubleType,true),
                                  StructField("generation_solar",DoubleType,true),StructField("generation_waste",DoubleType,true),
                                  StructField("generation_wind_onshore",DoubleType,true),StructField("generation_nuclear",DoubleType,true),
                                  StructField("generation_other",DoubleType,true),StructField("forecast_solar_day_ahead",DoubleType,true),
                                  StructField("forecast_wind_onshore_day_ahead",DoubleType,true),StructField("total_load_forecast",DoubleType,true),
                                  StructField("total_load_actual",DoubleType,true),StructField("price_day_ahead",DoubleType,true),
                                  StructField("price_actual",DoubleType,true),StructField("gen_fossil_totals",DoubleType,true),
                                  StructField("gen_nuclear_totals",DoubleType,true),StructField("gen_hydro_totals",DoubleType,true),
                                  StructField("gen_solar_totals",DoubleType,true),StructField("gen_wind_totals",DoubleType,true),
                                  StructField("gen_waste_totals",DoubleType,true),StructField("gen_renewable_totals",DoubleType,true),
                                  StructField("dt_iso",StringType,true),StructField("temp_Madrid",DoubleType,true),
                                  StructField("temp_min_Madrid",DoubleType,true),StructField("temp_max_Madrid",DoubleType,true),
                                  StructField("pressure_Madrid",DoubleType,true),StructField("humidity_Madrid",DoubleType,true),
                                  StructField("wind_speed_Madrid",DoubleType,true),StructField("wind_deg_Madrid",DoubleType,true),
                                  StructField("rain_1h_Madrid",DoubleType,true),StructField("rain_3h_Madrid",DoubleType,true),
                                  StructField("snow_3h_Madrid",DoubleType,true),StructField("clouds_all_Madrid",DoubleType,true),
                                  StructField("weather_id_Madrid",DoubleType,true),StructField("weather_main_Madrid",StringType,true),
                                  StructField("weather_description_Madrid",StringType,true),StructField("weather_icon_Madrid",StringType,true),
                                  StructField("temp_Seville",DoubleType,true),StructField("temp_min_Seville",DoubleType,true),
                                  StructField("temp_max_Seville",DoubleType,true),StructField("pressure_Seville",DoubleType,true),
                                  StructField("humidity_Seville",DoubleType,true),StructField("wind_speed_Seville",DoubleType,true),
                                  StructField("wind_deg_Seville",DoubleType,true),StructField("rain_1h_Seville",DoubleType,true),
                                  StructField("rain_3h_Seville",DoubleType,true),StructField("snow_3h_Seville",DoubleType,true),
                                  StructField("clouds_all_Seville",DoubleType,true),StructField("weather_id_Seville",DoubleType,true),
                                  StructField("weather_main_Seville",StringType,true),StructField("weather_description_Seville",StringType,true),
                                  StructField("weather_icon_Seville",StringType,true),StructField("temp_Bilbao",DoubleType,true),
                                  StructField("temp_min_Bilbao",DoubleType,true),StructField("temp_max_Bilbao",DoubleType,true),
                                  StructField("pressure_Bilbao",DoubleType,true),StructField("humidity_Bilbao",DoubleType,true),
                                  StructField("wind_speed_Bilbao",DoubleType,true),StructField("wind_deg_Bilbao",DoubleType,true),
                                  StructField("rain_1h_Bilbao",DoubleType,true),StructField("rain_3h_Bilbao",DoubleType,true),
                                  StructField("snow_3h_Bilbao",DoubleType,true),StructField("clouds_all_Bilbao",DoubleType,true),
                                  StructField("weather_id_Bilbao",DoubleType,true),StructField("weather_main_Bilbao",StringType,true),
                                  StructField("weather_description_Bilbao",StringType,true),StructField("weather_icon_Bilbao",StringType,true),
                                  StructField("temp_Barcelona",DoubleType,true),StructField("temp_min_Barcelona",DoubleType,true),
                                  StructField("temp_max_Barcelona",DoubleType,true),StructField("pressure_Barcelona",DoubleType,true),
                                  StructField("humidity_Barcelona",DoubleType,true),StructField("wind_speed_Barcelona",DoubleType,true),
                                  StructField("wind_deg_Barcelona",DoubleType,true),StructField("rain_1h_Barcelona",DoubleType,true),
                                  StructField("rain_3h_Barcelona",DoubleType,true),StructField("snow_3h_Barcelona",DoubleType,true),
                                  StructField("clouds_all_Barcelona",DoubleType,true),StructField("weather_id_Barcelona",DoubleType,true),
                                  StructField("weather_main_Barcelona",StringType,true),StructField("weather_description_Barcelona",StringType,true),
                                  StructField("weather_icon_Barcelona",StringType,true),StructField("temp_Valencia",DoubleType,true),
                                  StructField("temp_min_Valencia",DoubleType,true),StructField("temp_max_Valencia",DoubleType,true),
                                  StructField("pressure_Valencia",DoubleType,true),StructField("humidity_Valencia",DoubleType,true),
                                  StructField("wind_speed_Valencia",DoubleType,true),StructField("wind_deg_Valencia",DoubleType,true),
                                  StructField("rain_1h_Valencia",DoubleType,true),StructField("rain_3h_Valencia",DoubleType,true),
                                  StructField("snow_3h_Valencia",DoubleType,true),StructField("clouds_all_Valencia",DoubleType,true),
                                  StructField("weather_id_Valencia",DoubleType,true),StructField("weather_main_Valencia",StringType,true),
                                  StructField("weather_description_Valencia",StringType,true),StructField("weather_icon_Valencia",StringType,true),
                                  StructField("date",DateType,true),StructField("date_time",TimestampType,true)))

// COMMAND ----------

// MAGIC %md
// MAGIC ### Reading final dataset and creating temp views

// COMMAND ----------

val ener_weather_df = spark.read.format("csv")
      .option("header", "true")
      .schema(energy_weather_schema)
      .load("/FileStore/tables/finalEnergyWeatherData.csv")

println(f"full_df count : ${ener_weather_df.count}")
println(f"na_df count : ${ener_weather_df.na.drop.count}")

// COMMAND ----------

ener_weather_df.createOrReplaceTempView("ener_weather_table")
weather_df.createOrReplaceTempView("weather_table")
pred_select_df.createOrReplaceTempView("weather_predicted_ONLY_table")

// COMMAND ----------

// // Final check in scala
// ener_weather_df.na.drop.count // Should be == 35064
// weather_df.na.drop.count      // Should be == 35064
// pred_select_df.na.drop.count  // Should be == 2798


// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC -- select * from weather_table
// MAGIC -- select * from ener_weather_table
// MAGIC -- select * from weather_predicted_ONLY_table
