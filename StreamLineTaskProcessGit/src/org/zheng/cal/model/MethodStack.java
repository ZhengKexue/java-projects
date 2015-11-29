package org.zheng.cal.model;

import java.util.Date;
import java.util.List;

public class MethodStack {
	private String threadName;
	private Date time;
	private List<MethodStackElement> stackEles;
	public String getThreadName() {
		return threadName;
	}
	public void setThreadName(String threadName) {
		this.threadName = threadName;
	}
	public Date getTime() {
		return time;
	}
	public void setTime(Date time) {
		this.time = time;
	}
	public List<MethodStackElement> getStackEles() {
		return stackEles;
	}
	public void setStackEles(List<MethodStackElement> stackEles) {
		this.stackEles = stackEles;
	}
	
	
}
