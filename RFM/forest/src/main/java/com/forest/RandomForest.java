package com.forest;

import java.io.IOException;
import java.nio.file.Paths;
import java.nio.file.Files;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.StringIndexer;

public class RandomForest {
    public static void main(String[] args) {
        
        SparkSession spark = SparkSession.builder()
                .appName("RandomForest")
                .getOrCreate();

        //Format data
        Dataset<Row> data = spark.read().format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(args[0]);

        String labelCol = "Severity";

        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{
                "Temperature(F)",     // Column 20
                "Wind_Chill(F)",      // Column 21
                "Humidity(%)",        // Column 22
                "Pressure(in)",       // Column 23
                "Visibility(mi)",     // Column 24
                "Wind_Speed(mph)",    // Column 26
                "Precipitation(in)",  // Column 27
                "Amenity",            // Column 29
                "Crossing",           // Column 31
                "Junction",           // Column 33
                "Railway",            // Column 35
                "Station",            // Column 37
                "Stop",               // Column 38
                "Traffic_Calming"     // Column 40
                }).setOutputCol("features");


        RandomForestClassifier rf = new RandomForestClassifier()
                .setLabelCol(labelCol)
                .setFeaturesCol("features")
                .setNumTrees(10);  //Can change this

        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{assembler, rf});

        Dataset<Row>[] splits = data.randomSplit(new double[]{0.7, 0.3}, 1234); // Can also make a validation set
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];
        
        PipelineModel model = pipeline.fit(trainingData);

        //Save model
        try {
            Files.deleteIfExists(Paths.get(args[1])); //Overwrite
            model.save(args[1]);
        } catch (IOException e) {
            e.printStackTrace();
        }


        //Evalutate 
        Dataset<Row> predictions = model.transform(testData);

        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol(labelCol)
                .setPredictionCol("prediction")
                .setMetricName("accuracy");

        double accuracy = evaluator.evaluate(predictions);
        System.out.println("Test Accuracy = " + accuracy);

        
        spark.stop();
    }
}
