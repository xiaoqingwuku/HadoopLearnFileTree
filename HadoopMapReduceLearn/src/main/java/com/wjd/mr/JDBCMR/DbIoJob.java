package com.wjd.mr.JDBCMR;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//脚本运行
// export USERPATH=":/home/wangjianda/lib/*"
// export HADOOP_CLASSPATH=${HADOOOP_CLASSPATH}${USERPATH}
// hadoop jar ****.jar 

/**
 * 
 * JDBC读取数据库  
 * 
 **/
public class DbIoJob extends Configured implements Tool {
    //内部MAP类
	public static class DbInputMapper extends Mapper<LongWritable, DbUserWritable, DbUserDatWritable, NullWritable> {
   
		
		private DbUserDatWritable userdat = new DbUserDatWritable();

		@Override
		protected void map(LongWritable key, DbUserWritable value,
				Mapper<LongWritable, DbUserWritable, DbUserDatWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			userdat.setId(value.getId());
			userdat.setSalary(value.getId() * 10);// 随便设置一个整数，没有实际意义

			context.write(userdat, NullWritable.get());
		}

	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "DbIoJob");
		job.setJarByClass(DbIoJob.class);
		job.setMapperClass(DbInputMapper.class);
		job.setOutputKeyClass(DbUserDatWritable.class);
		job.setOutputValueClass(NullWritable.class);
		// 设置输入输出格式
		job.setInputFormatClass(DBInputFormat.class);
		job.setOutputFormatClass(DBOutputFormat.class);
		// 设置数据库连接信息
		DBConfiguration.configureDB(job.getConfiguration(), "com.mysql.jdbc.Driver",
				"jdbc:mysql://192.168.1.112:3306/mrtest", "root", "root");
		DBInputFormat.setInput(job, DbUserWritable.class, "select id,name from tbl_test",
				"select count(1) from tbl_test");
		DBOutputFormat.setOutput(job, "tbl_test", "id", "name");

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int res = ToolRunner.run(new DbIoJob(), args);
		System.exit(res);
		System.out.println("done");

	}

}