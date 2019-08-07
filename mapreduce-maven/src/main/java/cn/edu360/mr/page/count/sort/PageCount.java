package cn.edu360.mr.page.count.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class PageCount implements WritableComparable<PageCount>{

	private String page;
	private int count;
	
	
	public void set(String page, int count) {
		this.page = page;
		this.count = count;
	}
	public String getPage() {
		return page;
	}
	public void setPage(String page) {
		this.page = page;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	public int compareTo(PageCount o) {
		
		return o.getCount()-this.count==0?this.page.compareTo(o.getPage()):o.getCount()-this.count;
	}
	public void readFields(DataInput in) throws IOException {
		this.page=in.readUTF();
		this.count=in.readInt();
	}
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.page);
		out.writeInt(this.count);
	}
	@Override
	public String toString() {
		return "PageCount [page=" + page + ", count=" + count + "]";
	}
	
	
	
	
	
	
}
