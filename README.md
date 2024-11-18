# CS435-Term-Project
This is a repository for AJ Leichner, Wyatt Markham, Mason Stencel, and Maya Swarup CS435 term project
using the dataset found at [**DATASET**](https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents)

## What the project is about
This project does two main things it calucates and retrives the top 10 most average data points from the *US Accidents (2016 - 2023)*
data set. And there is a random forest model that can predict the severity of a crash. 

## How to Run
When you first pull these files you will need to run chmod +x *.sh for them to be 
executable 

The Compile.sh bash file compiles the project into a jar and puts it into hadoop
for this to work you need a to have a directory called CS435-Term-Project at root
so the file path /CS435-Term-Project should exist

The Run.sh bash file will run the job it will ask if you want yarn or local
and if you want to run the real data set or the sample. For this to work you
need the directories /CS435-Term-Project/Data and /CS435-Term-Project/Data_sample and
/CS435-Term-Project/output to exist

The Delete.sh will delete the output contents of the output directory so you can run a job

Inside of RFM is the random forest model you can simpily compile it using the compile command
inside of forest. You will have to rerun chmod +x *.sh once inside for it to work.
To run it you shouldn't have to change anything so long as you have the data files in the
struture described above. The run command will ask for your spark master node and port 
as well as your hadoop port. Then you can choose to Train the model or Test it. 
Training it will still test it but Test won't train. You will have to train a model
before you can run the test. 
