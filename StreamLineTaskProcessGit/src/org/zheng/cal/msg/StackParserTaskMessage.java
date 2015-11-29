package org.zheng.cal.msg;

import java.util.List;

import org.zheng.cal.model.MethodStack;
import org.zheng.cal.task.AbstractInTask;


public class StackParserTaskMessage extends AbstractMessage {

	private List<MethodStack>   methodStackList; 
	private String fileDir;
	
	//-----------
	public StackParserTaskMessage(AbstractInTask abstrctTask) {
		super(abstrctTask);
	}
	public StackParserTaskMessage() {
		// TODO Auto-generated constructor stub
	}
	public List<MethodStack> getMethodStackList() {
		return methodStackList;
	}
	public void setMethodStackList(List<MethodStack> methodStackList) {
		this.methodStackList = methodStackList;
	}
	public String getFileDir() {
		return fileDir;
	}
	public void setFileDir(String fileDir) {
		this.fileDir = fileDir;
	}
   
	
}
