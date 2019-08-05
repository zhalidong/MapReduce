package cn.edu360.hdfs.datacollect;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.TimerTask;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;


public class CollectTask extends TimerTask {

	@Override
	public void run() {
		/*
		 * 	--定时探测日志源目录
			--获取需要采集的文件
			--移动这些文件到一个待上传临时目录
			--遍历待上传目录中各文件  逐一传输到HDFS的目录路径 同时将传输完成的文件移动到备份目录
		 * 
		 * 
		 */
/*		//构造一个log4j日志对象
		Logger logger = LoggergetLogger("logRollingFile");*/
		
		
		
		//获取本次采集时的日期
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH");
		String day = sdf.format(new Date());
		
		
		
		File srcDir = new File("d:/logs/accesslog/");
		File[] listFiles = srcDir.listFiles(new FilenameFilter() {
			//列出日志源目录中需要采集的文件  过滤掉access.log文件
			@Override
			public boolean accept(File dir, String name) {
				if(name.startsWith("access.log.")){
					return true;
				}
				return false;
			}
		});
		//记录日志
//		logger.info("探测到如下文件需要采集："+Arrays.toString(listFiles));
		
		
		
		//将要采集的文件移动到待上传临时目录
		File toUploadDir = new File("d:/logs/toupload/");
		for (File file : listFiles) {
			file.renameTo(new File(" d:/logs/toupload/"));
		}
		
		//记录日志
//		logger.info("上述文件移动到了待上传目录"+toUploadDir.getAbsolutePath());
		
		
		//构造一个HDFS客户端对象
		try {
			FileSystem fs = FileSystem.get(new URI("hdfs://hdp-01:9000"), new Configuration(), "zkpk");
			File[] toUploadFiles = toUploadDir.listFiles();
			
			//检查HDFS中的日期目录是否存在  如果不存在 则创建
			Path hdfsDestPath = new Path("/logs/"+day);
			if(!fs.exists(hdfsDestPath)){
				fs.mkdirs(hdfsDestPath);
			}
			
			
			//检查本地的备份目录是否存在 如果不存在 则创建
			File backupDir = new File("d:/logs/backup/"+day+"/");
			if(!backupDir.exists()){
				backupDir.mkdirs();
			}
			
			
			for (File file : toUploadFiles) {
				//传输文件到hdfs并改名
				fs.copyFromLocalFile(new Path(file.getAbsolutePath()), new Path(hdfsDestPath+"/access_log_"+UUID.randomUUID()+".log"));
				//将传输完成的文件移动到备份目录
				file.renameTo(backupDir);
				
				
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
	}

}
