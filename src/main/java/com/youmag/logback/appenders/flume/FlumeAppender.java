/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache license, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the license for the specific language governing permissions and
 * limitations under the license.
 */
package com.youmag.logback.appenders.flume;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;

/**
 * An Appender that uses the Avro protocol to route events to Flume.
 */

public final class FlumeAppender extends AppenderBase<ILoggingEvent> {
    private ArrayList<FlumeAgent> agents = new ArrayList<FlumeAgent>();
    private AbstractFlumeManager manager = null;
    private String mdcIncludes = null;
    private String mdcExcludes = null;
    private String mdcRequired = null;
    private String eventPrefix = null;
    private String mdcPrefix = null;
    private boolean compressBody = false;
    private PatternLayout layout = null;
	private int batchSize = 1;
	private int reconnectDelay = 0;
	private int retries = 0;
	private String dataDir = null;
	private String type = "undef";
	
    /**
     * Create a Flume Avro Appender.
     */
    public FlumeAppender() {
    	super();       
    }

	/**
     * Publish the event.
     * @param event The ILoggingEvent.
     */
    public void append(final ILoggingEvent event) {

        final FlumeEvent flumeEvent = 
        		new FlumeEvent(event, mdcIncludes, mdcExcludes, 
        				mdcRequired, mdcPrefix,
        				eventPrefix, compressBody);
        
        String str = this.layout.doLayout(flumeEvent.getEvent());
        byte[] bytes = null;
        
        try {
        	bytes = str.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}        
		flumeEvent.setBody(bytes);
        manager.send(flumeEvent, reconnectDelay, retries);
    }

    /**
     * Start this appender.
     */
    @Override
    public void start() {
        if (layout == null) {
        	throw new RuntimeException("layout is null while creating appender !");
        }
        if (name == null) {
        	throw new RuntimeException("No name provided for Appender");
        }

        AbstractFlumeManager manager = null;

        if (agents == null || agents.size() == 0) {
        	addWarn("No agents provided, using defaults");
        	FlumeAgent defaultAgent = new FlumeAgent("localhost", 4141);
        	agents.add(defaultAgent);
        }

        if("avro".equals(type)) {
            manager = FlumeAvroManager.getManager(name, agents, batchSize);
        }

        if("agent".equals(type)) {
            manager = FlumeEmbeddedManager.getManager(name, agents, batchSize, dataDir);
        }
        
        if (manager == null) {
        	throw new RuntimeException("Could not build Flume manager, check your type");
        }
    	addWarn("Using manager " + type);

        this.manager = manager;
        super.start();
    }

    /**
     * Stop this appender.
     */
    @Override
    public void stop() {
        super.stop();
        manager.release();
    }

	public void addAgent(FlumeAgent agent) {
		this.agents.add(agent);
	}

	public void setManager(AbstractFlumeManager manager) {
		this.manager = manager;
	}

	public void setMdcIncludes(String mdcIncludes) {
		this.mdcIncludes = mdcIncludes;
	}

	public void setMdcExcludes(String mdcExcludes) {
		this.mdcExcludes = mdcExcludes;
	}

	public void setMdcRequired(String mdcRequired) {
		this.mdcRequired = mdcRequired;
	}

	public void setEventPrefix(String eventPrefix) {
		this.eventPrefix = eventPrefix;
	}

	public void setMdcPrefix(String mdcPrefix) {
		this.mdcPrefix = mdcPrefix;
	}

	public void setCompressBody(boolean compressBody) {
		this.compressBody = compressBody;
	}

	public void setLayout(PatternLayout layout) {
		this.layout = layout;
	}

	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	public void setReconnectDelay(int reconnectDelay) {
		this.reconnectDelay = reconnectDelay;
	}

	public void setRetries(int retries) {
		this.retries = retries;
	}

	public void setDataDir(String dataDir) {
		this.dataDir = dataDir;
	}

	public void setType(String type) {
		this.type = type;
	}

}
