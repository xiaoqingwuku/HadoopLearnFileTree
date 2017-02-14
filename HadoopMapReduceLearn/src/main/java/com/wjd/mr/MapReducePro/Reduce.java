package com.wjd.mr.MapReducePro;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.commons.lang.StringUtils;

public class Reduce extends Reducer<Text, Text, Text, Text> {

	private Text docIds = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		HashSet<Text> uniqueDocIds = new HashSet<Text>(); // 将同一个键对应的所有文档ID保存在同一个Set中

		for (Text docId : values) {

			uniqueDocIds.add(new Text(docId));

		}

		docIds.set(new Text(StringUtils.join(uniqueDocIds, ",")));// Collection
																  // 中值通过符号连接成字符串

		context.write(key, new Text(docIds));
		
	}

}
