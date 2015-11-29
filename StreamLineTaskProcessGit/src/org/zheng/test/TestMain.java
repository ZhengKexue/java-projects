package org.zheng.test;

import java.util.ArrayList;
import java.util.List;

import org.zheng.cal.msg.StackParserTaskMessage;
import org.zheng.cal.task.MasterTask;

/**
 * 
 * @author Administrator
 *
 */
public class TestMain {
	public static void main(String args[]) throws InterruptedException{
	 
		List<StackParserTaskMessage>  stackParserTaskMessageList=new ArrayList<StackParserTaskMessage> ();  ;
		buildMsgList(stackParserTaskMessageList,8);
		MasterTask masterTask = new MasterTask(stackParserTaskMessageList);
		Thread thread = new Thread(masterTask);
		thread.start();
		thread.join();
		System.out.println("done");
		
	}

	private static void buildMsgList(
			List<StackParserTaskMessage> stackParserTaskMessageList ,int index) {
		for(int i=0;i<index;i++){
			StackParserTaskMessage msg=new StackParserTaskMessage();
			msg.setFileDir("Java_Thread_Stack\\1");
			msg.setSourceTask(null);
			stackParserTaskMessageList.add(msg);
		}
	}

}
