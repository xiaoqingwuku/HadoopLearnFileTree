package com.wjd.mr.MapReducePro;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RunMain {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		runjob(args[0], args[1]);
	}

	public static void runjob(String input, String output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf);
		job.setJarByClass(RunMain.class);
		job.setMapperClass(Map.class); //指定Map
		job.setReducerClass(Reduce.class);//指定Reduce
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		//设置切分的最大值，最小值
		conf.setLong(FileInputFormat.SPLIT_MINSIZE, 1L); 
		conf.setLong(FileInputFormat.SPLIT_MAXSIZE, Long.MAX_VALUE);
		
		Path outputPath = new Path(output);
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath, true);
		job.waitForCompletion(true);

	}

}
