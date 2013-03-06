package com.youmag.logback.appenders.flume;

import org.junit.Test;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.core.util.StatusPrinter;

public class FlumeAppenderTest {

	@Test
	public void testFlumeAppender() {
		LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
	    		    
	    PatternLayout layout = new PatternLayout();
	    layout.setContext(loggerContext);
	    layout.setPattern("%d{yyyy-MM-dd HH:mm:ss} %c [%p] %m%n");
	    layout.start();

	    FlumeAgent agent1 = new FlumeAgent("marshall.youmag.com", 4141);

	    String type = "agent";
		int delay = 100;
		int agentRetries = 2;
		String name = "MyFlumeAppender";

		//		String dataDir = null;
		//		String dataDir = "InMemory";
		//		String excludes = null;
		//		String includes = null;
		//		String required = null;
		//		String mdcPrefix = null;
		//		String eventPrefix = null;
		//		boolean compressBody = false;
		//		int batchSize = 1;

		FlumeAppender fAppender = new FlumeAppender();

		fAppender.addAgent(agent1);
		fAppender.setType(type);
		fAppender.setReconnectDelay(delay);
		fAppender.setRetries(agentRetries);
		fAppender.setName(name);
		fAppender.setLayout(layout);

	   	fAppender.setContext(loggerContext);
	
	    fAppender.start();

	    // attach the rolling file appender to the logger of your choice
	    Logger logbackLogger = loggerContext.getLogger(FlumeAppenderTest.class);
	    logbackLogger.addAppender(fAppender);
	    
	    // OPTIONAL: print logback internal status messages
	    StatusPrinter.print(loggerContext);

	    // log something
	    logbackLogger.setLevel(Level.INFO);

	    logbackLogger.debug("this is a debugggg message");

	    logbackLogger.info("this is an info message");

	    logbackLogger.error("this is an error message");
	    
	    // wait a bit.
	    try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

