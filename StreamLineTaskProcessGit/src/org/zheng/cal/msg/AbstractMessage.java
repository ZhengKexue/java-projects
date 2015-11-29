package org.zheng.cal.msg;

import org.zheng.cal.task.AbstractInTask;

public abstract class AbstractMessage {

	protected AbstractInTask sourceTask;

	public AbstractInTask getSourceTask() {
		return sourceTask;
	}

	public void setSourceTask(AbstractInTask sourceTask) {
		this.sourceTask = sourceTask;
	}

	public AbstractMessage(AbstractInTask sourceTask) {
		super();
		this.sourceTask = sourceTask;
	}
	public AbstractMessage() {
		
	}
	
	 
}
