package cn.edu360.mr.wc.skew;

import java.io.IOException;
import java.util.Random;

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

import cn.edu360.mr.wc.JobSubmitterLinuxToYarn;
import cn.edu360.mr.wc.WordCountCombiner;
import cn.edu360.mr.wc.WordcountMapper;
import cn.edu360.mr.wc.WordcountReducer;

public class SkewWordcount {

	public static class SkewWordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		Random random=new Random();
		Text k = new Text();
		IntWritable v = new IntWritable(1);
		int numReduceTasks=0;
		
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			    numReduceTasks = context.getNumReduceTasks();
		}
		
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String[] words = value.toString().split(" ");
			for (String w : words) {
				
				k.set(w+"_"+random.nextInt(numReduceTasks));
				context.write(k, v);
			}
			
		}
		
		public static class SkewWordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
			IntWritable v = new IntWritable();
			@Override
			protected void reduce(Text key, Iterable<IntWritable> values,
					Context context)
					throws IOException, InterruptedException {
				
				int count=0;
				for (IntWritable value : values) {
					count+=value.get();
				}
				v.set(count);
				context.write(key, v);
				
			}
			
			
		}
		
		public static void main(String[] args) throws Exception {
			Configuration conf = new Configuration();
			
			Job job = Job.getInstance();
			
			job.setJarByClass(SkewWordcount.class);
			job.setMapperClass(SkewWordcountMapper.class);
			job.setReducerClass(SkewWordcountReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			
			
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			FileInputFormat.setInputPaths(job, new Path("d:/wordcount/input"));
			FileOutputFormat.setOutputPath(job,new Path("d:/wordcount/skew-out") );
			
			job.setNumReduceTasks(3);
			boolean res = job.waitForCompletion(true);
			System.exit(res?0:1);
			
		}
		
		
	}
	
	
	
	
}
