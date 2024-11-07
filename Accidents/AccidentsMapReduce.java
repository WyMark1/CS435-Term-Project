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
import org.apache.hadoop.io.LongWritable;
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
import Accidents.MapperWriteable;

public class AccidentsMapReduce extends Configured implements Tool {
	
	public static class getAveragesMapper extends Mapper<LongWritable, Text, IntWritable, MapperWriteable> {
		
		public void map(LongWritable key, Text text, Context context)
				throws IOException, InterruptedException {
				String rawText = text.toString();
				String[] lines = rawText.split("\\r?\\n\\r?\\n");
				int[] keptCols = {20,21,22,23,24,26,27,29,31,33,35,37,38,40};
				double[][] averages = new double[5][14];
				int[][] counts = new int[5][14];
				for (String line : lines) {
					String[] cols = line.split(",");
					if (cols[2].equals("") || cols[2].length() > 1) break;
					int severity = Integer.parseInt(cols[2]);
					for (int i = 0; i < keptCols.length; i++) {
						String val = cols[keptCols[i]].toLowerCase();
						double value = 0.0;
						if (!val.equals("")) {
							if (val.equals("true")) value = 1.0;
							else if (val.equals("false")) value = 0.0;
							else value  = Double.parseDouble(val);
							averages[severity][i] += value;
							averages[0][i] += value;
							counts[severity][i] += 1;
							counts[0][i] += 1;
						} 
					}
				}

				for (int i = 0; i < 5; i++) {
					IntWritable count = new IntWritable(i);
					MapperWriteable values = new MapperWriteable(averages[i], counts[i]);
					context.write(count, values);
				}
		}

	}

	public static class getTopNMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		public void map(LongWritable key, Text text, Context context)
				throws IOException, InterruptedException {
			//TODO
		}

	}

	public static class AveragesReducer extends Reducer<IntWritable, MapperWriteable, IntWritable, Text> {
		
		public void reduce(IntWritable key, Iterable<MapperWriteable> values, Context context)
				throws IOException, InterruptedException {
				double[] averages = new double[14];
				int[] counts = new int[14];
				for (MapperWriteable value : values) {
					double[] cols = value.getValues();
					int[] count = value.getCount();
					for (int i = 0; i < cols.length; i++) {
						averages[i] += cols[i];
						counts[i] += count[i];
					}
					
				}

				String finalVals = "";
				for(int i = 0; i < counts.length; i++) {
					finalVals += (averages[i]/counts[i]) + ", ";
				}

				Text output = new Text(finalVals);
				context.write(key, output);
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

		job1.setMapOutputKeyClass(IntWritable.class);
		job1.setMapOutputValueClass(MapperWriteable.class);
		job1.setOutputKeyClass(IntWritable.class); 
		job1.setOutputValueClass(Text.class);

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

		FileInputFormat.addInputPath(job2, new Path(inputDir));
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