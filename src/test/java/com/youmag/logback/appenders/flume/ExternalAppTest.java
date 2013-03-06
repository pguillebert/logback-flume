package com.youmag.logback.appenders.flume;

import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExternalAppTest {
	final static Logger logger = LoggerFactory.getLogger(ExternalAppTest.class);

	@Test
	public void testApplication() {
		logger.debug("Hello debug world.");

		logger.info("Hello info world.");

	    // wait a bit.
	    try {
			Thread.sleep(20000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
