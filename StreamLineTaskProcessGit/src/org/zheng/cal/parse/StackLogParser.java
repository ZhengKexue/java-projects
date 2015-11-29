package org.zheng.cal.parse;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.zheng.cal.model.MethodStack;
import org.zheng.cal.model.MethodStackElement;
import org.zheng.cal.msg.AbstractMessage;
import org.zheng.cal.msg.ProcessTaskMessage;
import org.zheng.cal.task.StackParserTask;

public class StackLogParser {

	private static Logger LOGGER= Logger.getLogger(StackLogParser.class);
	private int maxSize=50;
	public List<MethodStack> parse(File filePath ,StackParserTask currentTask) throws Exception {
	   List<MethodStack>   methodStackList=new ArrayList<MethodStack>(); 
		BufferedReader buffReader = null;
		try {
			buffReader = new BufferedReader(new FileReader(filePath));
			String line = null;
			MethodStack stack =null;
			while ((line = buffReader.readLine()) != null) {
				if(line.indexOf("at time:")>0){
                   //header	
				 if(methodStackList.size()==maxSize){
					 //1 .send methodStackList
					 currentTask.putMessageOutgoingQueue(createProcessTaskMessage(methodStackList));
					 // 2. re-new 
					 methodStackList=new ArrayList<MethodStack>();
				 }	
					
				  stack =  new MethodStack();
				  methodStackList.add(stack);
				  parseHeader(stack,line);
				}else{
					//ele
				  parseStack(stack,line);
				}
			}
			if(methodStackList.isEmpty()==false){
				currentTask.putMessageOutgoingQueue(createProcessTaskMessage(methodStackList));
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			throw e;
		}  finally {
			try {
				buffReader.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		
		return methodStackList;
	}

	private AbstractMessage createProcessTaskMessage(
			List<MethodStack> methodStackList) {
		// TODO Auto-generated method stub
		ProcessTaskMessage processTaskMessage=new ProcessTaskMessage ();
		processTaskMessage.setSourceTask(null);
		processTaskMessage.setMethodStackList(methodStackList);
		return processTaskMessage;
	}

	private void parseStack(MethodStack stack, String line) {
		List<MethodStackElement> stackEles = stack.getStackEles();
		if(stackEles==null){
			stackEles= new ArrayList<MethodStackElement>();
		}
		stackEles.add(new MethodStackElement(line));
		stack.setStackEles(stackEles);
		
	}

	private void parseHeader(MethodStack stack, String line) {
		// TODO Auto-generated method stub
		String timeString = line.substring(line.indexOf(':')).substring(0,line.indexOf("-------------------------------"));
		String threadName=line.substring(line.indexOf("Thread")).substring(0,line.indexOf("at"));
		try {
			stack.setTime(new SimpleDateFormat().parse(timeString));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
		}
		stack.setThreadName(threadName);
	}
	
}
