package org.zheng.cal.task;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.zheng.cal.msg.AbstractMessage;
import org.zheng.cal.msg.ENDTaskMessage;
import org.zheng.cal.msg.ExceptionTaskMessage;
import org.zheng.cal.msg.StackParserTaskMessage;
import org.zheng.cal.msg.StopTaskMessage;
/**
 * Background :
 *  ��ˮ�ߣ�pipeline��������ָ�ڳ���ִ��ʱ����ָ���ص����в�����һ��׼���д���ʵ�ּ�����
 *  ��ˮ����Intel�״���486оƬ�п�ʼʹ�õġ���ˮ�ߵĹ�����ʽ����ҵ�����ϵ�װ����ˮ�ߡ�
 *  ��CPU����5��6����ͬ���ܵĵ�·��Ԫ���һ��ָ�����ˮ�ߣ�Ȼ��һ��X86ָ��ֳ�5��6����������Щ��·��Ԫ�ֱ�ִ�У�
 *  ��������ʵ����һ��CPUʱ���������һ��ָ�������CPU�������ٶȡ����䱼��ÿ��������ˮ�߶���Ϊ�ļ���ˮ����ȡָ����롢ִ�С�д�ؽ����������ˮ�ַ�Ϊ�˼���ˮ��
 *  
 * StreamLineTaskProcess ���˼· :
 *  ���pipeline ����Ҫͨ��ͨ����ˮ�߽��������ݷֲ��������� �����ֲ� : ���ָ������ݴ�������׶� ������: ÿ���׶����ö���̲߳�������  ����
 * 
 * 
 * MasterTask ����:
 *        ������ˮ�ߵĴ��������١� �����������й���task���������� ��
 *        masterQueue ѭ������Ƿ���������Ϣ���������߳� �� StopTaskMessage/ExceptionTaskMessage
 *        1��StopTaskMessage �� ������remove��taskList.
 *        2)ExceptionTaskMessage ,ǿ����ֹ�����������е��̣߳���loop taskList���� isStop=true��.
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
 *
 *
 */
public class MasterTask implements Runnable{
	private static Logger LOGGER= Logger.getLogger(MasterTask.class);
	private int PARSERTASK_SIZE=2;
	private int PROCESSTASK_SIZE=2;
	
	protected BlockingQueue<AbstractMessage> masterQueue = new ArrayBlockingQueue<AbstractMessage>(100);
	private  List<StackParserTaskMessage>  stackParserTaskMessageList ;
	private ExecutorService exeService ;
	private List<AbstractInTask> taskList =new ArrayList<AbstractInTask>();
	
	
	
	public List<StackParserTaskMessage> getStackParserTaskMessage() {
		return stackParserTaskMessageList;
	}

	public void setStackParserTaskMessage(
			List<StackParserTaskMessage> stackParserTaskMessage) {
		this.stackParserTaskMessageList = stackParserTaskMessage;
	}

	
	public MasterTask(List<StackParserTaskMessage> stackParserTaskMessage) {
		super();
		this.stackParserTaskMessageList = stackParserTaskMessage;
		 exeService =Executors.newFixedThreadPool(10);
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
		// 1. init stackParseTask.

		BlockingQueue<AbstractMessage> stackParserIncomingQueue =new ArrayBlockingQueue<AbstractMessage>(100)  ;
		BlockingQueue<AbstractMessage> stackParserOutgoingQueue =new ArrayBlockingQueue<AbstractMessage>(100)  ;
		 try {
			if(stackParserTaskMessageList !=null){
				stackParserIncomingQueue.addAll(stackParserTaskMessageList);
				createStackParserTask(stackParserIncomingQueue,stackParserOutgoingQueue,masterQueue,exeService);
			}
			stackParserIncomingQueue.put(new ENDTaskMessage());// end flag
			
			waitOnOutgoingQueue(stackParserOutgoingQueue);
		 // 2 init ProcessTask.
			createProcessTask(stackParserOutgoingQueue,masterQueue,exeService);
			
		// 3.wait all task finish	
			while(true){
				 if(taskList.isEmpty()){
					 break; //finish normally.
				 }
				 checkQueueGroupStatus(stackParserIncomingQueue,
							stackParserOutgoingQueue, masterQueue);
				 waitOnMasterQueue(true);
			}
		//	
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LOGGER.debug(" got reported exception , stop all task"+ e );
			checkQueueGroupStatus(stackParserIncomingQueue,
					stackParserOutgoingQueue, masterQueue);
			stopAllTask();
		}finally{
			clearnQueue(stackParserIncomingQueue,
					stackParserOutgoingQueue, masterQueue);
//			while(exeService!=null){
//				try {
//					Thread.currentThread().sleep(3000);
//				} catch (InterruptedException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//				checkQueueGroupStatus(stackParserIncomingQueue,
//						stackParserOutgoingQueue, masterQueue);
//			}
			exeService.shutdown();
		}
	}

	private void clearnQueue(
			BlockingQueue<AbstractMessage> stackParserIncomingQueue,
			BlockingQueue<AbstractMessage> stackParserOutgoingQueue,
			BlockingQueue<AbstractMessage> masterQueue2) {
		stackParserIncomingQueue.clear();
		stackParserOutgoingQueue.clear();
		masterQueue2.clear();
		
	}

	private void stopAllTask() {
		for(AbstractInTask task:taskList){
			LOGGER.debug("try to set "+task+" is stop flag = true" );
			task.setStoped(true);
		}
		
		taskList.clear();
	}

	private void createProcessTask(
			BlockingQueue<AbstractMessage> stackParserOutgoingQueue,
			BlockingQueue<AbstractMessage> masterQueue,
			ExecutorService exeService2) {
		// TODO Auto-generated method stub
		AbstractInTask task=null;
		CountDownLatch counter =new CountDownLatch(PROCESSTASK_SIZE);
		for(int i=0;i<PROCESSTASK_SIZE;i++){
			task=new ProcessTask( stackParserOutgoingQueue, 
				 masterQueue,   counter);
			LOGGER.debug(task+" start");
			taskList.add(task);
			exeService2.submit(task);
		}
	}

	private void createStackParserTask(
			BlockingQueue<AbstractMessage> stackParserIncomingQueue,
			BlockingQueue<AbstractMessage> stackParserOutgoingQueue,
			BlockingQueue<AbstractMessage> masterQueue2,
			ExecutorService exeService2) {
		// TODO Auto-generated method stub
		AbstractInTask task=null;
		CountDownLatch counter =new CountDownLatch(PARSERTASK_SIZE);
		for(int i=0;i<PARSERTASK_SIZE;i++){
			task=new StackParserTask( stackParserIncomingQueue,stackParserOutgoingQueue,
				 masterQueue,   counter);
			LOGGER.debug(task+" start");
			taskList.add(task);
			exeService2.submit(task);
		}
	}

	private void waitOnOutgoingQueue(BlockingQueue<AbstractMessage> stackParserOutgoingQueue ) throws Exception {
		AbstractMessage eleMessage = null;
		while(true){
			eleMessage = stackParserOutgoingQueue.peek();
			LOGGER.debug("waitOnOutgoingQueue siz= "+stackParserOutgoingQueue.size()+", "+eleMessage);
			if(eleMessage ==null){
				waitOnMasterQueue(false);
				Thread.currentThread().sleep(100);
			}else{
				break;
			}
		}
		
	}
	
	private void waitOnMasterQueue(boolean blocked) throws Exception{
		AbstractMessage eleMessage =null ;
		if(!blocked) {
			eleMessage=masterQueue.poll();
		}else{
			eleMessage=masterQueue.take();
		}
		if(eleMessage!=null){
			if(eleMessage instanceof StopTaskMessage ){
				//
				StopTaskMessage stopMsg=(StopTaskMessage)eleMessage;
				stopMsg.getSourceTask().setStoped(true);
				taskList.remove(stopMsg.getSourceTask());
				LOGGER.debug(Thread.currentThread().getName()+" stop "+stopMsg.getSourceTask());
			}
			
			if(eleMessage instanceof ExceptionTaskMessage ){
				LOGGER.debug(" got reported exception from "+eleMessage.getSourceTask());
				throw ((ExceptionTaskMessage) eleMessage).getE();
			}
		}
	}


	public void checkQueueGroupStatus(
			BlockingQueue<AbstractMessage> stackParserIncomingQueue,
			BlockingQueue<AbstractMessage> stackParserOutgoingQueue,
			BlockingQueue<AbstractMessage> masterQueue) {
		LOGGER.debug("--------------------------------------------CheckQueueGroupStatus---------------------------------------------");
		LOGGER.debug(" stackParserIncomingQueue size:"+stackParserIncomingQueue.size() +" ,Detial :"+stackParserIncomingQueue );
		LOGGER.debug(" stackParserOutgoingQueue size:"+stackParserOutgoingQueue.size() +" ,Detial :"+stackParserOutgoingQueue );
		LOGGER.debug(" masterQueue size:"+masterQueue.size() +" ,Detial :"+masterQueue );
	}
}
