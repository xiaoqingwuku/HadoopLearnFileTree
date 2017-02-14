package com.wjd.mr.MapReducePro;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, Text, Text> {

	private Text word = new Text(); // 减少对象创建

	private Text documentId; // 存放文档名称（文件名）

	@Override
	protected void setup(Context context) {

		String fileNmae = ((FileSplit) context.getInputSplit()).getPath().getName();// 获取文件名称

		documentId = new Text(fileNmae);//赋值

	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		for (String token : StringUtils.split(value.toString())) { //空格分割

			word.set(token);

			context.write(word, documentId); //输出 

		}

	}

}