package org.zheng.cal.msg;

import org.zheng.cal.task.AbstractInTask;


public class StopTaskMessage extends AbstractMessage {

	public StopTaskMessage(AbstractInTask abstrctTask) {
		super(abstrctTask);
	}

}
