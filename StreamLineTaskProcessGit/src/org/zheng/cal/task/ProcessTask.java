package org.zheng.cal.task;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.zheng.cal.msg.AbstractMessage;
import org.zheng.cal.msg.ProcessTaskMessage;


public class ProcessTask extends  AbstractInTask{
	private static Logger LOGGER= Logger.getLogger(StackParserTask.class);
	protected ProcessTask(BlockingQueue<AbstractMessage> incomingQueue,
			BlockingQueue<AbstractMessage> masterQueue, CountDownLatch counter) {
		super(incomingQueue, masterQueue, counter);
	}

	@Override
	public void process(AbstractMessage inMsg) {
		ProcessTaskMessage processTaskMessage=(ProcessTaskMessage)inMsg;
		LOGGER.debug(" process task MethodStackList size:  "+processTaskMessage.getMethodStackList().size());
		LOGGER.debug(" process task MethodStackList belong to thread:  "+processTaskMessage.getMethodStackList().get(0).getThreadName());
		LOGGER.debug("\n");
		//write into lucene ;
		//sort 
		if(processTaskMessage.getMethodStackList().size()==38){
			throw new RuntimeException("---------- test exception --------");
		}
	}
	 
	
}
