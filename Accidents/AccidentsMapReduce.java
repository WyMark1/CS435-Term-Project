package Accidents;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.naming.Context;
import java.util.ArrayList;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import java.util.HashMap; 

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Writable;
import java.util.HashSet;
import org.apache.log4j.Logger;
import java.util.Map;
import java.lang.Math;


public class AccidentsMapReduce extends Configured implements Tool {
	
	public static class getAveragesMapper extends Mapper<Object, BytesWritable, Text, Text> {
		
		public void map(Object key, BytesWritable bWriteable, Context context)
				throws IOException, InterruptedException {
			//TODO
		}

	}

	public static class getTopNMapper extends Mapper<Object, BytesWritable, Text, Text> {
		
		public void map(Object key, BytesWritable bWriteable, Context context)
				throws IOException, InterruptedException {
			//TODO
		}

	}

	public static class AveragesReducer extends Reducer<Text, Text, Text, Text /*TODO*/> {
		
		public void reduce(Text text1, Iterable<Text> text2, Context context /*TODO*/)
				throws IOException, InterruptedException {
			//TODO
		}
	}

    public static class TopNReducer extends Reducer<Text, Text, Text, Text /*TODO*/> {
		
		public void reduce(Text text1, Iterable<Text> text2, Context context /*TODO*/)
				throws IOException, InterruptedException {
			//TODO
		}
	}

	public static int runJob(Configuration conf, String inputDir, String outputDir1, String outputDir2) throws Exception {
		
		Job job1 = Job.getInstance(conf, "Get Averages");

		job1.setJarByClass(AccidentsMapReduce.class);

		job1.setMapperClass(getAveragesMapper.class);
		job1.setReducerClass(AveragesReducer.class);

		job1.setOutputKeyClass(Text.class); //May need to change
		job1.setOutputValueClass(Text.class); //May need to change

		FileInputFormat.addInputPath(job1, new Path(inputDir));
		FileOutputFormat.setOutputPath(job1, new Path(outputDir1));

		if(!job1.waitForCompletion(true)) {
			return 1;
		}

		Job job2 = Job.getInstance(conf, "TFIDF");

		job2.setJarByClass(AccidentsMapReduce.class);

		job2.setMapperClass(getTopNMapper.class);
		job2.setReducerClass(TopNReducer.class);

		job2.setOutputKeyClass(Text.class); //May need to change
		job2.setOutputValueClass(Text.class); //May need to change

		FileInputFormat.addInputPath(job2, new Path(outputDir1));
		FileOutputFormat.setOutputPath(job2, new Path(outputDir2));

		if(!job2.waitForCompletion(true)) {
			return 1;
		} else {
			return 0;
		}
	}

	public static void main(String[] args) throws Exception {
		// ToolRunner allows for command line configuration parameters - suitable for
		// shifting between local job and yarn
		// example command: hadoop jar <path_to_jar.jar> <main_class> -D param=value
		// <input_path> <output_path>
		// We use -D mapreduce.framework.name=<value> where <value>=local means the job
		// is run locally and <value>=yarn means using YARN
		int res = ToolRunner.run(new Configuration(), new AccidentsMapReduce(), args);
		System.exit(res); // res will be 0 if all tasks are executed succesfully and 1 otherwise
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		if (runJob(conf, args[0], args[1], args[2]) != 0) {
			return -1; // Error
		}
		return 0; // success
	}
}