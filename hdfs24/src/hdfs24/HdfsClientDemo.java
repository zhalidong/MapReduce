package hdfs24;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.*;
import java.util.Arrays;

import javax.naming.InitialContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

public class HdfsClientDemo{
	
	public static void main(String[] args) throws Exception {
		/**
		 * Configuration参数对象的机制
		 * 构造时 会加载jar包中的默认配置xx-default.xml
		 * 再加载用户配置xx-site.xml	覆盖掉默认参数
		 * 构造完成后 还可以conf.set("p","v")会再次覆盖用户配置文件中的参数值
		 */
		
		
		//new Configuration() 会从项目的classpath中加载core-default.xml hdfs-default.xml core-site.xml hdfs-site.xml等文件
		Configuration conf = new Configuration();
		//指定本客户端上传文件到hdfs需要保存的副本数为2
		conf.set("dfs.replication", "2");
		//指定本客户端上传文件到hdfs时切块的规格大小:64M
		conf.set("dfs.blocksize", "64M");
		//构造一个访问指定hdfs系统的客户端对象：参数1 HDFS系统的URI 参数2 客户端要特别指定的参数 参数3: 客户端的身份(用户名) 
		FileSystem fs = FileSystem.get(new URI("hdfs://hdp-01:9000"), conf, "zkpk");
		
		//上传一个文件到hdfs中
		fs.copyFromLocalFile(new Path("D:/hbase-1.2.4-bin.tar.gz"), new Path("/aaa"));
		
		fs.close();
		
	}
	FileSystem fs = null;
	@Before
	public void init() throws Exception{
		//new Configuration() 会从项目的classpath中加载core-default.xml hdfs-default.xml core-site.xml hdfs-site.xml等文件
		Configuration conf = new Configuration();
		//指定本客户端上传文件到hdfs需要保存的副本数为2
		conf.set("dfs.replication", "2");
		//指定本客户端上传文件到hdfs时切块的规格大小:64M
		conf.set("dfs.blocksize", "64M");
		//构造一个访问指定hdfs系统的客户端对象：参数1 HDFS系统的URI 参数2 客户端要特别指定的参数 参数3: 客户端的身份(用户名) 
		fs = FileSystem.get(new URI("hdfs://hdp-01:9000"), conf, "zkpk");
		
	}
	/*
	 * 从hdfs中下载文件到客户端本地磁盘
	 */
	
	@Test
	public void testGet() throws IllegalArgumentException, IOException {
		 fs.copyToLocalFile(new Path("/aaa/hbase-1.2.4-bin.tar.gz"), new Path("D:\\"));
		 fs.close();
	}
	/*
	 * 在hdfs内部移动文件修改名称
	 */
	
	
	@Test
	public void testRename() throws Exception{
		fs.rename(new Path("/start-kafka.sh"), new Path("/aaa/kafka.sh"));
		fs.close();
	}
	
	/*
	 * 在hdfs中创建文件夹
	 */
	
	@Test
	public void testMkdir() throws Exception{
		fs.mkdirs(new Path("/xx/yy/zz"));
		fs.close();
	}
	/*
	 * 在hdfs中删除文件夹
	 */
	
	@Test
	public void testDelete() throws Exception{
		fs.delete(new Path("/aaa"),true);
		fs.close();
	}
	
	/*
	 *查询hdfs中指定目录下的信息
	 */
	
	@Test
	public void testSel() throws Exception{
		//只查询文件的信息  不显示文件夹信息
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		while(listFiles.hasNext()){
			LocatedFileStatus status = listFiles.next();
			System.out.println("文件全路径"+status.getPath());
			System.out.println("块大小"+status.getBlockSize());
			System.out.println("文件长度"+status.getLen());
			System.out.println("副本数量"+status.getReplication());
			System.out.println("块信息"+Arrays.toString(status.getBlockLocations()));
			System.out.println("-----------");
		}
		fs.close();
	}
	
	/*
	 * 读取hdfs中的文件的内容
	 * 
	 */
	@Test
	public void TestReadData() throws IllegalArgumentException, IOException{
		
		FSDataInputStream in = fs.open(new Path("/test.txt"));
		
		
		
		//字符流
		BufferedReader br = new BufferedReader(new InputStreamReader(in,"utf-8"));
		String line = "";
		while((line=br.readLine())!=null){
			System.out.println(line);
		}
		
		
		br.close();
		in.close();
		fs.close();
		
	}
	
	/*
	 * 读取hdfs中的文件指定偏移量的内容
	 * 作业题：用本例中的知识 实现读取一个文本文件中的指定BLOCK块中的所有数据
	 * 
	 */
	@Test
	public void TestRandomReadData() throws IllegalArgumentException, IOException{
		
		FSDataInputStream in = fs.open(new Path("/xx.dat"));
		
		//将读取的起始位置进行指定
		in.seek(12);
		//读16个字节
		byte[] b = new byte[16];
		in.read(b);
		
		System.out.println(new String(b));
		in.close();
		fs.close();
	}
	
	
	
	
	/*
	 * 往hdfs中的文件写内容
	 * 
	 */
	@Test
	public void testWriteData() throws IllegalArgumentException, IOException{
		FSDataOutputStream out = fs.create(new Path("/zz.jpg"),false);
		
		//D:\0db10c6a58935b5c365b0000792d75c3_t.jpg
		FileInputStream in = new FileInputStream("D:\0db10c6a58935b5c365b0000792d75c3_t.jpg");
		
		
		
		byte[] b = new byte[1024];
		int read=0;
		while((read=in.read(b))!=-1){
			out.write(b,0,read);
		}
		in.close();
		out.close();
		fs.close();
	}
	
}
