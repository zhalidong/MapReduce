package cn.edu360.mr.flow;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/*
 * 统计一下文件中，每一个用户所耗费的总上行流量，总下行流量，总流量
 * 
 */
public class JobSubmitter {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(JobSubmitter.class);
		job.setMapperClass(FlowCountMapper.class);
		job.setReducerClass(FlowCountReducer.class);
		//设置参数：map task 在做数据分区时用哪个分区逻辑类 如果不指定 它会默认用hashPartitioner
		job.setPartitionerClass(ProvincePartitioner.class);
		
		//由于我们的provincePartitioner可能会产生6种分区号  所有需要有6个reduce task 接收
		job.setNumReduceTasks(6);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		
		FileInputFormat.setInputPaths(job, new Path("d:\\mrdata\\flow\\input"));
		FileOutputFormat.setOutputPath(job, new Path("d:\\mrdata\\flow\\province-output"));
		
		job.waitForCompletion(true);
		
		
		
	}
}
