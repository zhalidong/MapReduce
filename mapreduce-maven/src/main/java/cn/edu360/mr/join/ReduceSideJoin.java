package cn.edu360.mr.join;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
/*
 * 5.1.mr编程案例8——join算法
 * 
 * 有订单数据：
 * order001,u001
	order002,u001
	order003,u005
	order004,u002
	order005,u003
	order006,u004
 * 
 * 有用户数据：
 * u001,senge,18,angelababy
	u002,laozhao,48,ruhua
	u003,xiaoxu,16,chunge
	u004,laoyang,28,zengge
 * 
 * 
 * 
 * 
 */
public class ReduceSideJoin {
	public static class ReduceSideJoinMapper extends Mapper<LongWritable, Text, Text, JoinBean>{
		String fileName=null;
		JoinBean bean = new JoinBean();
		Text k = new Text();
		/*
		 * map task在做数据处理时  会调用一次setup()
		 * 掉完后才对每一行反复调用map()
		 * 
		 * 
		 */
		
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, JoinBean>.Context context)
				throws IOException, InterruptedException {
			//切片文件信息
			FileSplit inputSplit = (FileSplit) context.getInputSplit();
			fileName = inputSplit.getPath().getName();
				
		}
		
		
		
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, JoinBean>.Context context)
				throws IOException, InterruptedException {
			
			String[] fields = value.toString().split(",");
			
			if(fileName.startsWith("order")){
				bean.set(fields[0], fields[1], "NULL", -1, "NULL", "order");
			}else {
				bean.set("NULL", fields[0], fields[1], Integer.parseInt(fields[2]), fields[3], "user");
			}
			k.set(bean.getUserId());
			context.write(k, bean);
			
			
		}
		
		public static class ReduceSideJoinReducer extends Reducer<Text, JoinBean, JoinBean, NullWritable>{
			
			@Override
			protected void reduce(Text key, Iterable<JoinBean> beans,
					Context context)
					throws IOException, InterruptedException {
					
				ArrayList<JoinBean> orderList = new ArrayList<JoinBean>();
				JoinBean userBean = null;
				try {//区分两类数据
					for (JoinBean bean : beans) {
						if(bean.getTableName().equals("order")){
							JoinBean newBean = new JoinBean();
								BeanUtils.copyProperties(newBean, bean);
							orderList.add(newBean);
							
						}else {
							userBean = new JoinBean();
							BeanUtils.copyProperties(userBean, bean);

						}
					}
					//拼接数据 并输出
					for (JoinBean bean : orderList) {
						bean.setUserName(userBean.getUserName());
						bean.setUserAge(userBean.getUserAge());
						bean.setUserFriend(userBean.getUserFriend());
						
						context.write(bean, NullWritable.get());
					}
					
					
					
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				} catch (InvocationTargetException e) {
					e.printStackTrace();
				}
				
			}
		}
		
	}
}
