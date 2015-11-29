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
 * ĩ������ , λ��������ˮ��ĩ�˵�task , ����û�� ����������
 * 
 * ����task���������� :
 * 
 *    ���(incoming.poll())��ѭ����� isStop ����isStop=false ֮���incoming��ȡ����Ϣ��
 *        1) ���������߳̽��ܵ�����������Ϣ(ENDTaskMessage)
 *    		 a) break loop ,�����Լ� ;
 *     		 b) �����Լ����ڵ�ͬ���������������ֵ������߳�δ�����������������Ϣ(ENDTaskMessage)�����Żص�ǰincoming���� ,���ݸ�ͬ���ֵ��߳� .
 *          	 �����ǰ�̷߳���ˮ��ĩ���߳� ��������ͬ���ֵ��̶߳�������ʱ�����������������Ϣ�ŵ�outgoing����
 *     		 c) ֪ͨ(StopTaskMessage)Master�߳��Լ��Ѿ����� .
 *        2) ���������߳̽��ܵ�������Ϣ,���й��̳����׳�Exception .
 *    		a)�׳�Exception �����Լ�
 *    		b)֪ͨMaster�̳߳��� ��ExceptionTaskMessage����Master�߳�ǿ����ֹ�����������е��̣߳���ͨ������ isStop=true��.
 *    
 *      ����(outgoing.offer())��ѭ����� isStop �����isStop=true ͨ���쳣�׳���������ǰ�̡߳�
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
