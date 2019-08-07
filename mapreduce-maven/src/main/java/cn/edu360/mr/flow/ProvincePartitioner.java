package cn.edu360.mr.flow;

import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
/*
 * 流量统计按归属地输出  -- 自定义partitioner
 * 
 * 本类是通过给map task 用的
 * map task 通过这个类的getPartition方法 来计算它所产生的每一对kv数据该分发给哪一个reduce task
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean>{
	static HashMap<String, Integer> codemap = new HashMap<String, Integer>();
	static{
		
		codemap.put("135", 0);
		codemap.put("136", 1);
		codemap.put("137", 2);
		codemap.put("138", 3);
		codemap.put("139", 4);
	}
	
	@Override
	public int getPartition(Text key, FlowBean value, int numPartitions) {
		
		Integer code = codemap.get(key.toString().substring(0, 3));
		
		
		return code==null?5:code;
	}

	
	
}
