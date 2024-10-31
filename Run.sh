#!/bin/bash
echo "Enter 's' for sample or 'r' for real dataset"
read choice
echo "Enter 'l' for local or 'y' for yarn"
read mode
if [[ "$choice" == "s" && "$mode" == "l" ]]; then
    hadoop jar Accidents.jar Accidents/AccidentsMapReduce -D param=local /CS435-Term-Project/Data_sample/* /CS435-Term-Project/output/Averages /CS435-Term-Project/output/TopN
elif [[ "$choice" == "s" && "$mode" == "y" ]]; then
    hadoop jar Accidents.jar Accidents/AccidentsMapReduce -D param=yarn /CS435-Term-Project/Data_sample/* /CS435-Term-Project/output/Averages /CS435-Term-Project/output/TopN
elif [[ "$choice" == "r" && "$mode" == "l" ]]; then
    hadoop jar Accidents.jar Accidents/AccidentsMapReduce -D param=local /CS435-Term-Project/Data/* /CS435-Term-Project/output/Averages /CS435-Term-Project/output/TopN
elif [[ "$choice" == "r" && "$mode" == "y" ]]; then
    hadoop jar Accidents.jar Accidents/AccidentsMapReduce -D param=yarn /CS435-Term-Project/Data/* /CS435-Term-Project/output/Averages /CS435-Term-Project/output/TopN
fi