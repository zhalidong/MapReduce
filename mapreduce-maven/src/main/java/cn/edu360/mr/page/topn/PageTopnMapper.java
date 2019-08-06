package cn.edu360.mr.page.topn;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//页面访问次数
public class PageTopnMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] split = line.split(" ");
		context.write(new Text(split[1]), new IntWritable(1));
		
	}

}
