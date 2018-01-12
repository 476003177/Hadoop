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
 *1.��������ҵ���߼���ȷ������������ݵ���ʽ
 *2.�Զ���һ���࣬�̳�org.apache.hadoop.mapreduce.Mapper����дmap������ʵ�־���ҵ���߼������µ�K��V���
 *3.�Զ���һ���࣬�̳�org.apache.hadoop.mapreduce.Reducer����дreduce������ʵ�־���ҵ���߼������µ�K��V���
 *4.���Զ����mapper��reducerͨ��job������װ����
 */

public class WordCount {

	public static void main(String[] args) throws Exception {
		//����job����
		Job job=Job.getInstance(new Configuration());
		//ע�⣺main����������
		job.setJarByClass(WordCount.class);
		
		//����hadoop�Զ����Mapper�������������
		job.setMapperClass(WCMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		FileInputFormat.setInputPaths(job, new Path("/words.txt"));
		//����hadoop�Զ����Reducer�������������
		job.setReducerClass(WCReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileOutputFormat.setOutputPath(job, new Path("/wcout222"));
		
		//�趨Combiner���ɿ���С�͵�reducer�����п���
		job.setCombinerClass(WCReducer.class);
		//�ύ����
		job.waitForCompletion(true);//true��ӡ�������飬false����ӡ
		
	}
}
