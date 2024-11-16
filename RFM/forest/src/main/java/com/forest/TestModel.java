package com.forest;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.tuning.CrossValidatorModel.CrossValidatorModelWriter;
import org.apache.spark.ml.tuning.CrossValidatorModel;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

public class TestModel {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("TestModel")
                .getOrCreate();

        Dataset<Row> testData = spark.read().format("csv")
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

        Dataset<Row> processedData = assembler.transform(testData);


        CrossValidatorModel model = CrossValidatorModel.load(args[1]);
        Dataset<Row> predictions = model.transform(processedData);
        Dataset<Row> outData = predictions.select("prediction", inputAttributes);
        // outData.show();
        outData.write().mode(SaveMode.Overwrite).format("csv").save(args[2]);


        // Dataset<Row> outData1 = processedData.select(labelCol);
        // outData1.write().mode(SaveMode.Overwrite).format("csv").save("/actual");

        // Dataset<Row> outData2 = predictions.select("prediction");
        // outData2.write().mode(SaveMode.Overwrite).format("csv").save("/prediction");

        spark.stop();
    }
}
