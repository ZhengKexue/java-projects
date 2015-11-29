package org.zheng.cal.task;

import java.io.File;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.zheng.cal.msg.AbstractMessage;
import org.zheng.cal.msg.StackParserTaskMessage;
import org.zheng.cal.parse.StackLogParser;


public class StackParserTask extends  AbstractInOutTask{
	private static Logger LOGGER= Logger.getLogger(StackParserTask.class);
	protected StackParserTask(BlockingQueue<AbstractMessage> incomingQueue,
			BlockingQueue<AbstractMessage> outcomingQueue,
			BlockingQueue<AbstractMessage> masterQueue, CountDownLatch counter) {
		super(incomingQueue, outcomingQueue,masterQueue, counter);
	}

	@Override
	public void process(AbstractMessage inMsg) throws Exception {
		StackParserTaskMessage msg=(StackParserTaskMessage)inMsg;
		// fileDir
		File fileDir =new File(msg.getFileDir());
		LOGGER.debug("start parse folder :["+fileDir+']');
		if(fileDir.isDirectory()){
			for(File f:fileDir.listFiles() ){
				if(f.isFile()){
					LOGGER.debug("start parse file :["+fileDir+'\\'+f.getName()+']');
					new StackLogParser().parse(f, this);
					LOGGER.debug("finish parse file :["+fileDir+'\\'+f.getName()+']');
				}
			}
		}
	}
	 
	
}
