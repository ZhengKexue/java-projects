package org.zheng.cal.task;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.zheng.cal.exception.StopTaskIndException;
import org.zheng.cal.msg.AbstractMessage;
import org.zheng.cal.msg.ENDTaskMessage;
import org.zheng.cal.msg.ExceptionTaskMessage;
import org.zheng.cal.msg.StopTaskMessage;
/**
 * 
 * 末端任务 , 位于整个流水线末端的task , 后面没有 其他的任务。
 * 
 * 工作task的生命周期 :
 * 
 *    入口(incoming.poll())：循环检测 isStop ，如isStop=false 之后从incoming中取得消息。
 *        1) 工作任务线程接受到正常结束消息(ENDTaskMessage)
 *    		 a) break loop ,结束自己 ;
 *     		 b) 根据自己所在的同级任务组的如果有兄弟任务线程未结束，将任务结束消息(ENDTaskMessage)继续放回当前incoming队列 ,传递给同级兄弟线程 .
 *          	 如果当前线程非流水线末端线程 ，当所有同级兄弟线程都结束的时候如果将结束任务消息放到outgoing队列
 *     		 c) 通知(StopTaskMessage)Master线程自己已经结束 .
 *        2) 工作任务线程接受到任务消息,运行过程出错抛出Exception .
 *    		a)抛出Exception 结束自己
 *    		b)通知Master线程出错 （ExceptionTaskMessage），Master线程强制终止所有正在运行的线程，（通过设置 isStop=true）.
 *    
 *      出口(outgoing.offer())：循环检测 isStop ，如果isStop=true 通过异常抛出来结束当前线程。
 * 
 * @author Kexue_Zheng
 */
public abstract class AbstractInTask implements Runnable {
	private static Logger LOGGER= Logger.getLogger(AbstractInTask.class);
	protected boolean isStoped = false;
	protected BlockingQueue<AbstractMessage> incomingQueue;
	protected BlockingQueue<AbstractMessage> masterQueue;
	protected CountDownLatch counter;
	protected Thread masterThread;
	
	
	protected AbstractInTask(BlockingQueue<AbstractMessage> incomingQueue,BlockingQueue<AbstractMessage> masterQueue,
			CountDownLatch counter) {
		super();
		this.incomingQueue = incomingQueue;
		this.counter = counter;
		this.masterQueue=masterQueue;
	}

	@Override
	public void run() {
		AbstractMessage inMsg=null;
		try {
			while (!isStoped) {
				
			    //inMsg=incomingQueue.take() ; take method may block thread when meet exception , current thread can't stop normally!
				inMsg=incomingQueue.poll(100, TimeUnit.MICROSECONDS);
			    if(inMsg==null){
			    	Thread.currentThread().yield();
			    	Thread.sleep(100);
			    }else if( inMsg instanceof ENDTaskMessage){
			    	processEndTaskMessage(inMsg);
			    	isStoped=true;
			    	break;
			    }
			    
				try {
					process(inMsg);
				}catch ( StopTaskIndException e) {
					throw e;
				} catch (Exception e) {
					// TODO Auto-generated catch block
					LOGGER.error (" got exception as below stack:" ,e);
					throw e;
				}
			}
			LOGGER .debug("current thread stopped by master normally");
		}catch ( StopTaskIndException e) {
			// master task want to stop current thread ,so got StopTaskIndException 
			LOGGER.warn("current thread stopped by master ( other task report exception) ");
		}  catch ( Exception e) {
			LOGGER.error("current thread stopped by got exception and exit "+e.getMessage());
			try {
				processException(e );
			} catch (InterruptedException e1) {
				// while report exception to master queue got exception, need directly  interrupt master thread . 
				masterThread.interrupt();
				e1.printStackTrace();
			}
		} 

	}
    /**
     * report exception to master queue.
     * @param e
     * @throws InterruptedException
     */
	protected void processException(Exception e ) throws InterruptedException {
			masterQueue.put(new ExceptionTaskMessage(this ,e));
		
	}

	protected void processEndTaskMessage(AbstractMessage inMsg) throws InterruptedException {
		synchronized (counter) {
			counter.countDown();
			LOGGER.debug(Thread.currentThread().getName()+" got EndMSg try to stoped");
			masterQueue.put(new StopTaskMessage(this));
			if(counter.getCount()>0){
				// if bother exist send to bother 
				waitOffer(inMsg,incomingQueue);
			}
		}
		
	}
	/**
	 * loop offer inMsg to queue and check stop flag;
	 * @param inMsg
	 * @throws InterruptedException 
	 */
	protected void waitOffer(AbstractMessage inMsg,
			BlockingQueue<AbstractMessage> queue) throws InterruptedException {
		while (queue.offer(inMsg, 20, TimeUnit.MICROSECONDS)) {
			if (this.isStoped()) {
				throw new StopTaskIndException();
			}
		}
	}

	public abstract void process(AbstractMessage inMsg) throws Exception;

	public boolean isStoped() {
		return isStoped;
	}

	public void setStoped(boolean isStoped) {
		this.isStoped = isStoped;
	}

	public Thread getMasterThread() {
		return masterThread;
	}

	public void setMasterThread(Thread masterThread) {
		this.masterThread = masterThread;
	}

}
