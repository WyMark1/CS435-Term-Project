package com.forest;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class TestModel {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("TestModel")
                .getOrCreate();

        PipelineModel model = PipelineModel.load(args[1]);

        Dataset<Row> testData = spark.read().format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(args[0]);

    
        Dataset<Row> predictions = model.transform(testData);

        predictions.select("prediction", "label").show();

        predictions.write().format("csv").save(args[2]);

        spark.stop();
    }
}
