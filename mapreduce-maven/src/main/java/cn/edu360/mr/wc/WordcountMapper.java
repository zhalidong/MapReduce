package cn.edu360.mr.wc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/*
 * KEYIN:是map task读取到的数据的key的类型 是一行的起始偏移量
 * VALUEIN:是map task读取到的数据的value的类型 是一行的内容string
 * 
 * KEYOUT:是用户的自定义map方法要返回的结果kv数据的key的类型 在wordcount逻辑中 我们需要返回的是单词string
 * VALUEOUT:是用户的自定义map方法要返回的结果kv数据的value的类型  在wordcount逻辑中 我们需要返回的是整数Integer
 * 
 *
 * 注意：
 * 	但是在mapreduce中 map产生的数据需要传输给reduce 需要进行序列化和反序列化 而jdk中的原生态序列化机制产生的数据量比较冗余 就会导致数据在mapreduce运行过程中传输效率低下
 * 所以hadoop专门设计了自己的序列化机制 那么mapreduce中传输的数据类型就必须实现hadoop自己的序列化接口
 * 
 * hadoop为JDK中常用的基本类型 long string Integer float 等数据类型封装了自己的实现了hadoop序列化接口的类型：LongWritable,Text,IntWritable,FloatWritable
 * 
 */
public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	//map此方法是map task调用
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		//切单词
		String line = value.toString();
		String[] words = line.split(" ");
		
		for(String word:words){
			context.write(new Text(word),new IntWritable(1) );
			
		}
		
	}
	
	
	
	
}
