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
 *  流水线（pipeline）技术是指在程序执行时多条指令重叠进行操作的一种准并行处理实现技术。
 *  流水线是Intel首次在486芯片中开始使用的。流水线的工作方式就象工业生产上的装配流水线。
 *  在CPU中由5—6个不同功能的电路单元组成一条指令处理流水线，然后将一条X86指令分成5—6步后再由这些电路单元分别执行，
 *  这样就能实现在一个CPU时钟周期完成一条指令，因此提高CPU的运算速度。经典奔腾每条整数流水线都分为四级流水，即取指令、译码、执行、写回结果，浮点流水又分为八级流水。
 *  
 * StreamLineTaskProcess 设计思路 :
 *  借鉴pipeline ，主要通过通过流水线将批量数据分步并发处理 。（分步 : 划分复杂数据处理到多个阶段 ；并发: 每个阶段启用多个线程并发处理  ）。
 * 
 * 
 * MasterTask 流程:
 *        负责流水线的创建，销毁。 管理其他所有工作task的生命周期 。
 *        masterQueue 循环检测是否有以下消息来至任务线程 ： StopTaskMessage/ExceptionTaskMessage
 *        1）StopTaskMessage ， 将任务remove出taskList.
 *        2)ExceptionTaskMessage ,强制终止所有正在运行的线程，（loop taskList设置 isStop=true）.
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
