#!/bin/bash
echo "Enter your spark master node"
read master
echo "Enter your spark port"
read port
echo "Enter your hadoop port"
read hport
echo "Enter 'Test' to test the model and 'Train' to Train"
read choice
echo "Enter 's' for sample or 'r' for real dataset"
read data
if [[ "$choice" == "Train" && "$data" == "s" ]]; then
    spark-submit --class com.forest.RandomForest --master spark://$master.cs.colostate.edu:$port target/forest-1.0-SNAPSHOT.jar hdfs://$master.cs.colostate.edu:$hport/CS435-Term-Project/Data_sample/* Model
elif [[ "$choice" == "Train" && "$data" == "r" ]]; then
    spark-submit --class com.forest.RandomForest --master spark://$master.cs.colostate.edu:$port target/forest-1.0-SNAPSHOT.jar hdfs://$master.cs.colostate.edu:$hport/CS435-Term-Project/Data/* Model
elif [[ "$choice" == "Test" && "$data" == "s" ]]; then
    spark-submit --class com.forest.TestModel --master spark://$master.cs.colostate.edu:$port target/forest-1.0-SNAPSHOT.jar hdfs://$master.cs.colostate.edu:$hport/CS435-Term-Project/Data_sample/* Model Predictions
elif [[ "$choice" == "Test" && "$data" == "r" ]]; then
    spark-submit --class com.forest.TestModel --master spark://$master.cs.colostate.edu:$port target/forest-1.0-SNAPSHOT.jar hdfs://$master.cs.colostate.edu:$hport/CS435-Term-Project/Data/* Model Predictions
fi