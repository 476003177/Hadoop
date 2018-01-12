package cn.itcast.hadoop.hdfs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

public class HDFSDemo {
	
	FileSystem fs=null;
	
	@Before
	public void init() throws IOException, URISyntaxException, InterruptedException{
		//����FileSystemʵ���ࣨ�����ࣩ����αװ��root
		fs=FileSystem.get(new URI("hdfs://itcast01:9000"), new Configuration(),"root");
	}
	
	@Test
	public void testUpload() throws Exception{
		//��ȡ�����ļ�ϵͳ���ļ�������������
		InputStream in = new FileInputStream("f://test.txt");
		//��HDFS�ϴ���һ���ļ������������
		OutputStream out = fs.create(new Path("/test.txt"));
		//����->���
		IOUtils.copyBytes(in, out, 4096,true);
	}
	
	@Test
	public void testDownload() throws Exception, IOException{
		fs.copyToLocalFile(new Path("/jdk1.7"), new Path("f://jdk1.7"));
	
	}
	@Test
	public void testDel() throws Exception, Exception {
		boolean mkdir=fs.delete(new Path("/jdk1.7"), false);//falseΪ�ǵݹ�ɾ���������ǿ��ļ���ܾ�ɾ��
		System.out.println(mkdir);
	}
	
	@Test
	public void testMkdir() throws Exception{
		boolean mkdirs=fs.mkdirs(new Path("/itcast0106"));
	}
	
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		FileSystem fs=FileSystem.get(new URI("hdfs://itcast01:9000"), new Configuration());
		InputStream in = fs.open(new Path("/jdk1.7"));
		OutputStream out = new FileOutputStream("f://jdk1.7");
		IOUtils.copyBytes(in, out, 4090, true);
		
	}

}
