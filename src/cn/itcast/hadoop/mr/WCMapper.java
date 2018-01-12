package cn.itcast.hadoop.mr;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
	//LongWritable��Text��hadoop�����л��࣬��Ӧjava��Long��String����java�����л�����̫���࣬����Ҫ��hadoop�Դ���
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//��������V1��value
		String line=value.toString();
		//�з�����
		String[] words=line.split(" ");
		//ѭ��
		for(String w: words){
			//����һ�Σ���һ��1�����
			context.write(new Text(w), new LongWritable(1));
		}
	}
}
