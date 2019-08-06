package cn.edu360.mr.page.topn;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.edu360.mr.flow.FlowBean;
import cn.edu360.mr.flow.FlowCountMapper;
import cn.edu360.mr.flow.FlowCountReducer;

public class JobSubmitter {
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		//代码设置参数 conf.setInt("top.n", 3);
		//读取配置文件  通过属性配置文件获取参数
		Properties props = new Properties();
		props.load(JobSubmitter.class.getClassLoader().getResourceAsStream("topn.properties"));
		conf.setInt("topn", Integer.parseInt(props.getProperty("top.n")));
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(JobSubmitter.class);
		job.setMapperClass(PageTopnMapper.class);
		job.setReducerClass(PageTopnReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job, new Path("d:\\mrdata\\url\\input"));
		FileOutputFormat.setOutputPath(job, new Path("d:\\mrdata\\url\\output"));
		job.waitForCompletion(true);
		
	}
}
