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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class AccidentsMapReduce extends Configured implements Tool {
	
	public static class getAveragesMapper extends Mapper<LongWritable, Text, Text, MapperWriteable> {
		
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
					Text count = new Text(i + "");
					MapperWriteable values = new MapperWriteable(averages[i], counts[i]);
					context.write(count, values);
				}
		}

	}

	public static class getTopNMapper extends Mapper<LongWritable, Text, Text, Text> {
		private double[][] severityAverages = new double[5][14]; // [severity][feature]

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			// Load averages from the distributed cache into the 2D averages array
			Path[] cacheFiles = context.getLocalCacheFiles();
			if (cacheFiles != null && cacheFiles.length > 0) {
				for (Path cachePath : cacheFiles) {
					try (BufferedReader reader = new BufferedReader(new FileReader(cachePath.toString()))) {
						String line;
						while ((line = reader.readLine()) != null) {
							String[] tokens = line.split("\t");
							int severity = Integer.parseInt(tokens[0]);
							String[] avgValues = tokens[1].split(", ");
							
							for (int i = 0; i < avgValues.length; i++) { // Populate averages row
								severityAverages[severity][i] = Double.parseDouble(avgValues[i]);
							}
						}
					}
				}
			}
		}
		
		public void map(LongWritable key, Text text, Context context)
				throws IOException, InterruptedException {
			String line = text.toString();
			String[] cols = line.split(",");

			if (cols[2].equals("") || cols[2].length() > 1) return; // Check proper severity level entry
			int severity = Integer.parseInt(cols[2]);

			int[] keptCols = {20, 21, 22, 23, 24, 26, 27, 29, 31, 33, 35, 37, 38, 40};
			
			double deviation = 0.0;
			double globalDeviation = 0.0;
			int count = 0;

			for (int i = 0; i < keptCols.length; i++) {
				String val = cols[keptCols[i]].toLowerCase();
				double value = 0.0;
				if (!val.equals("")) { // Similar to getAverageMapper implementation
					if (val.equals("true")) value = 1.0;
					else if (val.equals("false")) value = 0.0;
					else value = Double.parseDouble(val);

					deviation += Math.abs(value - severityAverages[severity][i]); // Calculate deviation from average for the record's severity

					globalDeviation += Math.abs(value - severityAverages[0][i]); // Calculate deviation from global averages

					count++;
				}
			}
			
			double[] values = {deviation, globalDeviation};
			int[] counts = {count};

			String serializedOutput = deviation + "," + globalDeviation + "," + count;

			context.write(new Text(severity + "_" + key.toString()), new Text(serializedOutput));
		}

	}

	public static class AveragesReducer extends Reducer<Text, MapperWriteable, Text, Text> {
		
		public void reduce(Text key, Iterable<MapperWriteable> values, Context context)
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

    public static class TopNReducer extends Reducer<Text, Text, Text, Text> {

		int N = 10; // TOP N VALUE
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			TreeMap<Double, Text> globalDeviations = new TreeMap<>(Comparator.reverseOrder());
			TreeMap<Double, Text> severity1Deviations = new TreeMap<>(Comparator.reverseOrder());
			TreeMap<Double, Text> severity2Deviations = new TreeMap<>(Comparator.reverseOrder());
			TreeMap<Double, Text> severity3Deviations = new TreeMap<>(Comparator.reverseOrder());
			TreeMap<Double, Text> severity4Deviations = new TreeMap<>(Comparator.reverseOrder());


			for (Text value : values) {

				String[] parts = value.toString().split(",");
				String[] keys = key.toString().split("_");

				String severity = keys[0];
				String recordId = keys[1];
				double globalDeviation = Double.parseDouble(parts[1]);
				double severityDeviation = Double.parseDouble(parts[0]);

				globalDeviations.put(globalDeviation, new Text(value));
				if (globalDeviations.size() > N)
					globalDeviations.remove(globalDeviations.firstKey());
				
				switch(severity) {
					case "1":
						severity1Deviations.put(severityDeviation, new Text(value));
						if (severity1Deviations.size() > N)
							severity1Deviations.remove(severity1Deviations.firstKey());
						break;
					case "2":
						severity2Deviations.put(severityDeviation, new Text(value));
						if (severity2Deviations.size() > N)
							severity2Deviations.remove(severity2Deviations.firstKey());
						break;
					case "3":
						severity3Deviations.put(severityDeviation, new Text(value));
						if (severity3Deviations.size() > N)
							severity3Deviations.remove(severity3Deviations.firstKey());
						break;
					case "4":
						severity4Deviations.put(severityDeviation, new Text(value));
						if (severity4Deviations.size() > N)
							severity4Deviations.remove(severity4Deviations.firstKey());
						break;
					default:
						throw new IOException("Invalid severity level: " + severity);
				}
				// double[] deviations = value.getValues(); // [severityDeviation, globalDeviation]
				// int recordId = Integer.parseInt(key.toString().split("_")[1]);
				
				// // global deviation
				// double globalDeviation = deviations[1];
				// TreeMap<Double, Integer> globalMap = topDeviationsMap.get(0);
				// globalMap.put(globalDeviation, recordId);
				// if (globalMap.size() > N)
				// 	globalMap.pollLastEntry();
				
				// // severity-specific deviation
				// int severity = Integer.parseInt(key.toString().split("_")[0]);
				// double severityDeviation = deviations[0];
				// TreeMap<Double, Integer> severityMap = topDeviationsMap.get(severity);
				// severityMap.put(severityDeviation, recordId);
				// if (severityMap.size() > N)
				// 	severityMap.pollLastEntry();

				// Split the serialized string to extract deviations and count
				// String[] parts = value.toString().split(",");
				// double severityDeviation = Double.parseDouble(parts[0]);
				// double globalDeviation = Double.parseDouble(parts[1]);
				// int count = Integer.parseInt(parts[2]);

				// int recordId = Integer.parseInt(key.toString().split("_")[1]);
				// int severity = Integer.parseInt(key.toString().split("_")[0]);

				// // Add global deviation to global (severity level 0) map and ensure only top N are kept
				// TreeMap<Double, Integer> globalMap = topDeviationsMap.get(0);
				// globalMap.put(globalDeviation, recordId);
				// if (globalMap.size() > N)
				// 	globalMap.pollLastEntry();

				// // Add severity-specific deviation to the map for this severity level
				// TreeMap<Double, Integer> severityMap = topDeviationsMap.get(severity);
				// severityMap.put(severityDeviation, recordId);
				// if (severityMap.size() > N)
				// 	severityMap.pollLastEntry();


			}

			// int writeCount = 1;
			// context.write(new Text("Global Deviations top " + N + ":"), new Text(""));
			// for (Map.Entry<Double, Integer> entry : topDeviationsMap.get(0).entrySet()) {
			// 	if (writeCount > N)
			// 		break;
			// 	context.write(new Text("Record ID " + entry.getValue()), new Text("Deviation: " + entry.getKey()));
			// 	writeCount++;
			// }

			// for (int severity = 1; severity <= 4; severity++) {
			// 	context.write(new Text("Severity " + severity + " Deviations top " + N + ":"), new Text(""));
			// 	writeCount = 1;
			// 	for (Map.Entry<Double, Integer> entry : topDeviationsMap.get(severity).entrySet()) {
			// 		if (writeCount > N)
			// 			break;
			// 		co
					
			// 		ntext.write(new Text("Record ID " + entry.getValue()), new Text("Deviation: " + entry.getKey()));
			// 		writeCount++;
			// 	}
			// }
			// context.write(new Text("Global Deviations top " + N + ":"), new Text());
			// for (Text t : globalDeviations.descendingMap().values())
			// 	context.write(key, t);

			// context.write(new Text("Severity 1 Deviations top " + N + ":"), new Text());
			// for (Text t : severity1Deviations.descendingMap().values())
			// 	context.write(key, t);

			// context.write(new Text("Severity 2 Deviations top " + N + ":"), new Text());
			// for (Text t : severity2Deviations.descendingMap().values())
			// 	context.write(key, t);

			// context.write(new Text("Severity 3 Deviations top " + N + ":"), new Text());
			// for (Text t : severity3Deviations.descendingMap().values())
			// 	context.write(key, t);

			// context.write(new Text("Severity 4 Deviations top " + N + ":"), new Text());
			// for (Text t : severity4Deviations.descendingMap().values())
			// 	context.write(key, t);

			context.write(new Text("Global Deviations top " + N + ":"), new Text());

			int rank = 1;
			for (Map.Entry<Double, Text> entry : globalDeviations.descendingMap().entrySet()) {
				if (rank > N) break; // Ensure we only take the top N
				String recordId = entry.getValue().toString().split(",")[0]; // Extract record ID only
				double globalDeviation = entry.getKey();
				context.write(new Text(rank + "_" + recordId), new Text(Double.toString(globalDeviation)));
				rank++;
			}

			context.write(new Text("Severity 1 Deviations top " + N + ":"), new Text());
			rank = 1;
			for (Map.Entry<Double, Text> entry : severity1Deviations.descendingMap().entrySet()) {
				if (rank > N) break;
				String recordId = entry.getValue().toString().split(",")[0];
				double severityDeviation = entry.getKey();
				context.write(new Text(rank + "_" + recordId), new Text(Double.toString(severityDeviation)));
				rank++;
			}

			context.write(new Text("Severity 2 Deviations top " + N + ":"), new Text());
			rank = 1;
			for (Map.Entry<Double, Text> entry : severity2Deviations.descendingMap().entrySet()) {
				if (rank > N) break;
				String recordId = entry.getValue().toString().split(",")[0];
				double severityDeviation = entry.getKey();
				context.write(new Text(rank + "_" + recordId), new Text(Double.toString(severityDeviation)));
				rank++;
			}

			context.write(new Text("Severity 3 Deviations top " + N + ":"), new Text());
			rank = 1;
			for (Map.Entry<Double, Text> entry : severity2Deviations.descendingMap().entrySet()) {
				if (rank > N) break;
				String recordId = entry.getValue().toString().split(",")[0];
				double severityDeviation = entry.getKey();
				context.write(new Text(rank + "_" + recordId), new Text(Double.toString(severityDeviation)));
				rank++;
			}

			context.write(new Text("Severity 4 Deviations top " + N + ":"), new Text());
			rank = 1;
			for (Map.Entry<Double, Text> entry : severity2Deviations.descendingMap().entrySet()) {
				if (rank > N) break;
				String recordId = entry.getValue().toString().split(",")[0];
				double severityDeviation = entry.getKey();
				context.write(new Text(rank + "_" + recordId), new Text(Double.toString(severityDeviation)));
				rank++;
			}
		}
	}

	public static int runJob(Configuration conf, String inputDir, String outputDir1, String outputDir2) throws Exception {
		
		Job job1 = Job.getInstance(conf, "Get Averages");

		job1.setJarByClass(AccidentsMapReduce.class);

		job1.setMapperClass(getAveragesMapper.class);
		job1.setReducerClass(AveragesReducer.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(MapperWriteable.class);
		job1.setOutputKeyClass(Text.class); 
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

		// Explicitly set the number of reducers to 1 for TopNReducer
		job2.setNumReduceTasks(1);

		job2.setOutputKeyClass(Text.class); //May need to change
		job2.setOutputValueClass(Text.class); // May need to change

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