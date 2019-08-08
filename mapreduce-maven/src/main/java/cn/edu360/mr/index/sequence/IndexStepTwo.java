package cn.edu360.mr.index.sequence;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.edu360.mr.flow.FlowBean;
import cn.edu360.mr.flow.FlowCountMapper;
import cn.edu360.mr.flow.FlowCountReducer;
import cn.edu360.mr.flow.JobSubmitter;
import cn.edu360.mr.flow.ProvincePartitioner;

public class IndexStepTwo {

	public static class IndexStepTwoMapper extends Mapper<Text, IntWritable, Text, Text>{
		@Override
		protected void map(Text key, IntWritable value, Context context)
				throws IOException, InterruptedException {
			
			String[] split = key.toString().split("-");
			context.write(new Text(split[0]), new Text(split[1]+"-->"+value));
		}
		
	}
	
	public static class IndexStepTwoReducer extends Reducer<Text, Text, Text, Text>{
		//一组数据  ：  <hello,a.txt-->4> <hello,b.txt-->4> <hello,c.txt-->4>
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			
			
			//stringbuffer是线程安全的 stringbuilder是非线程安全的  在不涉及线程安全的场景下 stringbuilder更快
			StringBuilder sb = new StringBuilder();
			for (Text value : values) {
				sb.append(value.toString()).append("\t");
			}
			
			context.write(key, new Text(sb.toString()));
			
		}
	}
		public static void main(String[] args) throws Exception {
			Configuration conf = new Configuration();		//默认只加载core-default.xml  core-site.xml
			Job job = Job.getInstance(conf);
			job.setJarByClass(IndexStepTwo.class);
			job.setMapperClass(IndexStepTwoMapper.class);
			job.setReducerClass(IndexStepTwoReducer.class);
			
			job.setNumReduceTasks(3);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			//设置文件输入格式为sequence
			job.setInputFormatClass(SequenceFileInputFormat.class);
			
			FileInputFormat.setInputPaths(job, new Path("d:\\mrdata\\index\\out1"));
			FileOutputFormat.setOutputPath(job, new Path("d:\\mrdata\\index\\out2"));
			
			job.waitForCompletion(true);
			
			
		}
}
