package cn.edu360.mr.order.topn.grouping;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderIdGroupingComparator extends WritableComparator{
	
	public OrderIdGroupingComparator(){
		super(OrderBean.class,true);
		
	}
	
	
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		OrderBean o1 = (OrderBean) a;
		OrderBean o2 = (OrderBean) b;
		
		return o1.getOrderId().compareTo(o2.getOrderId());
	}
	
	

}
