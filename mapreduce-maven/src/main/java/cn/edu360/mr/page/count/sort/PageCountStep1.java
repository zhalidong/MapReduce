package cn.edu360.mr.page.count.sort;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.edu360.mr.page.topn.JobSubmitter;
import cn.edu360.mr.page.topn.PageTopnMapper;
import cn.edu360.mr.page.topn.PageTopnReducer;

public class PageCountStep1 {
	
	public static class PageCountStep1Mapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] words = line.split(" ");
			context.write(new Text(words[1]), new IntWritable(1));
			
		}
		
	}
	public static class PageCountStep1Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int count=0;
			for (IntWritable v : values) {
				count+=v.get();
			}
			context.write(key, new IntWritable(count));
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(PageCountStep1.class);
		job.setMapperClass(PageCountStep1Mapper.class);
		job.setReducerClass(PageCountStep1Reducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job, new Path("d:\\mrdata\\url\\input"));
		FileOutputFormat.setOutputPath(job, new Path("d:\\mrdata\\url\\countoutput"));
		job.setNumReduceTasks(3);
		job.waitForCompletion(true);
		
		
	}
}














