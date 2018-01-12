package cn.itcast.hadoop.mr;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * @author A
 *1.分析具体业务逻辑，确定输入输出数据的样式
 *2.自定义一个类，继承org.apache.hadoop.mapreduce.Mapper，重写map方法，实现具体业务逻辑，将新的K、V输出
 *3.自定义一个类，继承org.apache.hadoop.mapreduce.Reducer，重写reduce方法，实现具体业务逻辑，将新的K、V输出
 *4.将自定义的mapper和reducer通过job对象组装起来
 */

public class WordCount {

	public static void main(String[] args) throws Exception {
		//构建job对象
		Job job=Job.getInstance(new Configuration());
		//注意：main方法所在类
		job.setJarByClass(WordCount.class);
		
		//告诉hadoop自定义的Mapper并配置输入输出
		job.setMapperClass(WCMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		FileInputFormat.setInputPaths(job, new Path("/words.txt"));
		//告诉hadoop自定义的Reducer并配置输入输出
		job.setReducerClass(WCReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileOutputFormat.setOutputPath(job, new Path("/wcout222"));
		
		//设定Combiner，可看作小型的reducer，可有可无
		job.setCombinerClass(WCReducer.class);
		//提交任务
		job.waitForCompletion(true);//true打印进度详情，false不打印
		
	}
}
