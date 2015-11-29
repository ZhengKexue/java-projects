package org.zheng.cal.task;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.zheng.cal.msg.AbstractMessage;
import org.zheng.cal.msg.StopTaskMessage;
/**
 * 
 * 过程任务 , 位于整个流水线开始或者过程中间task , 后面还有其他的任务。
 * @author Kexue_Zheng
 *
 */
public abstract class AbstractInOutTask extends AbstractInTask {
	private static Logger LOGGER= Logger.getLogger(AbstractInOutTask.class);
	protected BlockingQueue<AbstractMessage> outcomingQueue;
	
	
	protected AbstractInOutTask(BlockingQueue<AbstractMessage> incomingQueue,
			BlockingQueue<AbstractMessage> outcomingQueue,
			BlockingQueue<AbstractMessage> masterQueue,
			CountDownLatch counter) {
		super(incomingQueue, masterQueue, counter);
		this.outcomingQueue=outcomingQueue;
	}

	protected void processEndTaskMessage(AbstractMessage inMsg) throws InterruptedException {
		synchronized (counter) {
			counter.countDown();
			LOGGER.debug(Thread.currentThread().getName()+" got EndMSg try to stoped");
			masterQueue.put(new StopTaskMessage(this));
			if(counter.getCount()>0){
//				incomingQueue.put(inMsg);
				waitOffer(inMsg,incomingQueue);
			}else{
				LOGGER.debug(Thread.currentThread().getName()+"  found  without bother thread need stop ");
//				outcomingQueue.put(inMsg);
				waitOffer(inMsg,outcomingQueue);
				LOGGER.debug(Thread.currentThread().getName()+"found  without bother thread need stop , put ot next parse "+inMsg+" to " +outcomingQueue );
			}
		}
		
	}
	 
	
	public void putMessageOutgoingQueue(AbstractMessage msg) throws InterruptedException{
//			this.outcomingQueue.put( msg);
			waitOffer( msg,outcomingQueue);
	}

}
