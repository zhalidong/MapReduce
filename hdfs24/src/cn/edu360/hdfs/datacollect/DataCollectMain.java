package cn.edu360.hdfs.datacollect;

import java.util.Timer;

public class DataCollectMain {
	public static void main(String[] args) {
		//线程调度任务以供将来在后台线程中执行的功能。 任务可以安排一次执行，或定期重复执行
		Timer timer1 = new Timer();
		//在指定 的延迟之后开始 ，重新执行 固定延迟执行的指定任务。 
		timer1.schedule(new CollectTask(), 0, 60*60*1000L);
		
		
		
		
		
	}
}
