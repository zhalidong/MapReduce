package cn.edu360.hdfs.wordcount;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class HdfsWordCount {
	
	public static void main(String[] args) throws Exception {
		
		
		/*
		 * 初始化工作
		 */
		
		//利用反射加载配置文件
		Properties props = new Properties();
		props.load(HdfsWordCount.class.getClassLoader().getResourceAsStream("job.properties"));
		
		Path intput = new Path(props.getProperty("INPUT_PATH"));
		Path ontput = new Path(props.getProperty("OUTPUT_PATH"));
		
		
		//反射技术拿到配置文件中的参数名
		Class<?> mapper_class = Class.forName(props.getProperty("MAPPER_CLASS"));
		//创建实例
		Mapper mapper = (Mapper) mapper_class.newInstance();
		
		Context context = new Context();
		
		/*
		 * 处理数据
		 */
		FileSystem fs = FileSystem.get(new URI("hdfs://hdp-01:9000"), new Configuration(), "zkpk");
		//目录中有哪些文件   返回一个迭代器
		RemoteIterator<LocatedFileStatus> iter = fs.listFiles(intput, false);
		
		while(iter.hasNext()){
			//拿到一个文件
			LocatedFileStatus file = iter.next();
			//打开流
			FSDataInputStream in = fs.open(file.getPath());
			//读取文本文件的流
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			//去hdfs中读文件 一次读一行
			String line = null;
			while((line=br.readLine())!=null){
				//调用一个方法对每一行进行业务处理
				mapper.map(line, context);	
				
			}
			br.close();
			in.close();
		}
		
		/*
		 * 输出结果
		 */
		HashMap<Object, Object> contextMap = context.getContextMap();
		//判断hdfs上目录是否存在
		if(fs.exists(ontput)){
			throw new RuntimeException("指定的输出目录已存在");
		}
		
		
		//输出流
		FSDataOutputStream out = fs.create(new Path(ontput,new Path("res.dat")));
		Set<Entry<Object, Object>> entrySet = contextMap.entrySet();
		for (Entry<Object, Object> entry : entrySet) {
			out.write((entry.getKey().toString()+"\t"+entry.getValue()+"\n").getBytes());
		}
		out.close();
		fs.close();
		System.out.println("数据统计完成...");
	}
}
