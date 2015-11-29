package org.zheng.cal.msg;

import org.zheng.cal.task.AbstractInTask;


public class ExceptionTaskMessage extends AbstractMessage {

	 private Exception e;
	public ExceptionTaskMessage(AbstractInTask abstrctTask) {
		super(abstrctTask);
	}
	
	public ExceptionTaskMessage(AbstractInTask abstrctTask,Exception e) {
		super(abstrctTask);
		this.e=e;
	}

	public Exception getE() {
		return e;
	}

	public void setE(Exception e) {
		this.e = e;
	}
	
}
