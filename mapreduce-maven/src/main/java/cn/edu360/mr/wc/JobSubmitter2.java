package cn.edu360.mr.wc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//此代码打包到linux下运行  在hadoop集群上启动提交客户端  conf就不需要指定fs.defaultFS 
/*
 * hadoop jar mapreduce-maven-0.0.1-SNAPSHOT.jar cn.edu360.mr.wc.JobSubmitter2
 * 使用hadoop jar 会加载hadoop中的依赖包 和配置文件
 */
public class JobSubmitter2 {

	public static void main(String[] args) throws Exception {
		//没指定默认文件系统   没指定mapreduce-job提交到哪运行
		Configuration conf = new Configuration();
		Job job = Job.getInstance();
		
		job.setJarByClass(JobSubmitter2.class);
		job.setMapperClass(WordcountMapper.class);
		job.setReducerClass(WordcountReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));
		FileOutputFormat.setOutputPath(job,new Path("/wordcount/output") );
		
		job.setNumReduceTasks(3);
		boolean res = job.waitForCompletion(true);
		System.exit(res?0:1);
	}
	
}
