package cn.itcast.hadoop.mr;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
	//LongWritable和Text是hadoop的序列化类，对应java的Long和String，但java的序列化方法太冗余，所以要用hadoop自带的
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//接收数据V1即value
		String line=value.toString();
		//切分数据
		String[] words=line.split(" ");
		//循环
		for(String w: words){
			//出现一次，记一个1，输出
			context.write(new Text(w), new LongWritable(1));
		}
	}
}
