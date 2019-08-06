package cn.edu360.mr.flow;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * 对每个人的手机号求流量总和(上限流量和下限流量)
 * 
 */
public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] fields = line.split("\t");
		
		String phone = fields[1];
		//把字符串转换成int
		int upFlow = Integer.parseInt(fields[fields.length-3]);
		int dFlow = Integer.parseInt(fields[fields.length-2]);
		
		context.write(new Text(phone), new FlowBean(phone, upFlow, dFlow));
		
	}
	
}
