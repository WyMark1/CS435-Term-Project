# CS435-Term-Project
This is a repository for our CS435 term project

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
