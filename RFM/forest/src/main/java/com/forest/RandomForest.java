package com.forest;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.TreeMap;
import java.nio.file.Files;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.linalg.Vector;

public class RandomForest {

    public static void printData(String output, String mode, SparkSession spark, String filename) {
        System.out.println(output);

        Dataset<Row> df = spark.createDataFrame(Collections.singletonList(RowFactory.create(output)),
                                                new StructType().add("data", DataTypes.StringType, true));
        df.write().mode(mode).text(filename);
        
    }

    public static void main(String[] args) {
        
        String outFilename = args[2];

        SparkSession spark = SparkSession.builder()
                .appName("RandomForest")
                .getOrCreate();

        //Ingest and format data
        Dataset<Row> data = spark.read().format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(args[0]);
        
        String labelCol = "Severity";

        String[] inputAttributes = new String[]{
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
                };

        VectorAssembler assembler = new VectorAssembler()
                .setHandleInvalid("skip")
                .setInputCols(inputAttributes).setOutputCol("features");

        Dataset<Row> processedData = assembler.transform(data);

        // Split data into train and test datasets
        double trainRatio = 0.7;
        double testRatio = 0.3;

        String output = "Splitting dataset: " + trainRatio + " Train and " + testRatio + " Test\n";
        printData(output, "overwrite", spark, outFilename);

        Dataset<Row>[] splits = processedData.randomSplit(new double[]{trainRatio, testRatio}); // Can also make a validation set
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];

        // Build model
        RandomForestClassifier rf = new RandomForestClassifier()
        .setLabelCol(labelCol)
        .setFeaturesCol("features");

        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol(labelCol)
                .setPredictionCol("prediction")
                .setMetricName("accuracy");

        ParamGridBuilder paramGrid = new ParamGridBuilder()
                .addGrid(rf.numTrees(), new int[]{30,40})
                .addGrid(rf.maxDepth(), new int[]{11,15});

        CrossValidator crossValidator = new CrossValidator()
                .setEstimator(rf)
                .setEvaluator(evaluator)
                .setEstimatorParamMaps(paramGrid.build())
                .setNumFolds(5);

        output = "Training Random Forest model on training dataset with 5-fold cross-validation\n";
        printData(output, "append", spark, outFilename);

        // Train models and select best model
        CrossValidatorModel model = crossValidator.fit(trainingData);

        ParamMap[] pmap = model.getEstimatorParamMaps();
        double[] metrics = model.avgMetrics();
        for (int i=0; i<pmap.length; i++) {
                output = "Parameter combination: " + pmap[i] + " has average cross-validation accuracy: " + metrics[i] + "\n";
                printData(output, "append", spark, outFilename);
        }

        RandomForestClassificationModel rfcModel = (RandomForestClassificationModel) model.bestModel();

        output = "\nThe best model has parameters: NumTrees = " + rfcModel.getNumTrees() 
                        + ";  MaxDepth = " + rfcModel.getMaxDepth() + "\n";
        printData(output, "append", spark, outFilename);


        Vector tuple = rfcModel.featureImportances();
        TreeMap<String,Double> featureImportance = new TreeMap<>();
        for (int i=0; i < tuple.size(); i++) {
                featureImportance.put(inputAttributes[i], tuple.apply(i));
        }

        output = "Attribute contributions: " + featureImportance + "\n";
        printData(output, "append", spark, outFilename);

        //Save model
        try {
            output = "Saving best random forest model to : " + Paths.get(args[1] + "\n");
            printData(output, "append", spark, outFilename);
            Files.deleteIfExists(Paths.get(args[1])); //Overwrite
            model.write().overwrite().save(args[1]);
        } catch (IOException e) {
            output = "Error saving best random forest model: " + e.toString();
            printData(output, "append", spark, outFilename);
            e.printStackTrace();
        }

        //Evalutate best model on Test dataset
        Dataset<Row> predictions = model.transform(testData);

        double accuracy = evaluator.evaluate(predictions);
        output = "Accuracy of best random forest model on Test dataset is: " + accuracy + "\n";
        printData(output, "append", spark, outFilename);

        spark.stop();
    }
}
