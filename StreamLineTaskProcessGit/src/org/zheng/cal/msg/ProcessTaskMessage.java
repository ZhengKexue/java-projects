package org.zheng.cal.msg;

import java.util.List;

import org.zheng.cal.model.MethodStack;


public class ProcessTaskMessage extends AbstractMessage {
	private List<MethodStack>   methodStackList;

	public List<MethodStack> getMethodStackList() {
		return methodStackList;
	}

	public void setMethodStackList(List<MethodStack> methodStackList) {
		this.methodStackList = methodStackList;
	} 
}
