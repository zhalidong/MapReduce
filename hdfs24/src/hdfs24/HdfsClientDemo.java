package hdfs24;

import java.io.IOException;
import java.net.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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
}
