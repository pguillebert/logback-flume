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

import org.apache.flume.SourceRunner;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.node.NodeConfiguration;
import org.apache.flume.node.nodemanager.DefaultLogicalNodeManager;

import ch.qos.logback.core.LogbackException;

import java.util.List;
import java.util.Properties;

public class FlumeEmbeddedManager extends AbstractFlumeManager {
    /** Name for the Flume source */
    protected static final String SOURCE_NAME = "logback-source";

    private static FlumeManagerFactory factory = new FlumeManagerFactory();

    private static final String FiLE_SEP = PropertiesUtil.getProperties().getStringProperty("file.separator");

    private static final String IN_MEMORY = "InMemory";

    private final FlumeNode node;

    private final LogbackEventSource source;

    private final String shortName;


    /**
     * Constructor
     * @param name The unique name of this manager.
     * @param node The Flume Node.
     */
    protected FlumeEmbeddedManager(final String name, final String shortName, final FlumeNode node) {
        super(name);
        this.node = node;
        this.shortName = shortName;
        final SourceRunner runner = node.getConfiguration().getSourceRunners().get(SOURCE_NAME);
        if (runner == null || runner.getSource() == null) {
            throw new IllegalStateException("No Source has been created for Appender " + this.shortName);
        }
        source  = (LogbackEventSource) runner.getSource();
    }

    /**
     * Returns a FlumeEmbeddedManager.
     * @param name The name of the manager.
     * @param agents The agents to use.
     * @param properties Properties for the embedded manager.
     * @param batchSize The number of events to include in a batch.
     * @param dataDir The directory where the Flume FileChannel should write to.
     * @return A FlumeAvroManager.
     */
    public static FlumeEmbeddedManager getManager(final String name, final List<FlumeAgent> agents,
                                                  int batchSize, final String dataDir) {

        if (batchSize <= 0) {
            batchSize = 1;
        }

        if ((agents == null || agents.size() == 0)) {
            throw new IllegalArgumentException("Agents are required");
        }

        final StringBuilder sb = new StringBuilder();
        boolean first = true;

        if (agents != null && agents.size() > 0) {
            sb.append("FlumeEmbedded[");
            for (final FlumeAgent agent : agents) {
                if (!first) {
                    sb.append(",");
                }
                sb.append(agent.getHost()).append(":").append(agent.getPort());
                first = false;
            }
            sb.append("]");
        }
        return (FlumeEmbeddedManager) getManager(sb.toString(), factory,
            new FactoryData(name, agents, batchSize, dataDir));
    }

    @Override
    public void send(final FlumeEvent event, final int delay, final int retries) {
        source.send(event);
    }

    @Override
    protected void releaseSub() {
        node.stop();
    }

    /**
     * Factory data.
     */
    private static class FactoryData {
        private final List<FlumeAgent> agents;
        private final int batchSize;
        private final String dataDir;
        private final String name;

        /**
         * Constructor.
         * @param name The name of the Appender.
         * @param agents The agents.
         * @param properties The Flume configuration properties.
         * @param batchSize The number of events to include in a batch.
         * @param dataDir The directory where Flume should write to.
         */
        public FactoryData(final String name, final List<FlumeAgent> agents, final int batchSize,
                           final String dataDir) {
            this.name = name;
            this.agents = agents;
            this.batchSize = batchSize;
            this.dataDir = dataDir;
        }
    }

    /**
     * Avro Manager Factory.
     */
    private static class FlumeManagerFactory implements ManagerFactory<FlumeEmbeddedManager, FactoryData> {
        private static final String SOURCE_TYPE = LogbackEventSource.class.getName();

        /**
         * Create the FlumeAvroManager.
         * @param name The name of the entity to manage.
         * @param data The data required to create the entity.
         * @return The FlumeAvroManager.
         */
        public FlumeEmbeddedManager createManager(final String name, final FactoryData data) {
            try {
                final DefaultLogicalNodeManager nodeManager = new DefaultLogicalNodeManager();
                final Properties props = createProperties(data.name, data.agents, data.batchSize,
                    data.dataDir);
                final FlumeConfigurationBuilder builder = new FlumeConfigurationBuilder();
                final NodeConfiguration conf = builder.load(data.name, props, nodeManager);

                final FlumeNode node = new FlumeNode(nodeManager, nodeManager, conf);

                node.start();

                return new FlumeEmbeddedManager(name, data.name, node);
            } catch (final Exception ex) {
            	throw new LogbackException("Could not create FlumeEmbeddedManager", ex);
            }
        }

        private Properties createProperties(final String name, final List<FlumeAgent> agents,
                                            final int batchSize, String dataDir) {
            final Properties props = new Properties();

            if ((agents == null || agents.size() == 0)) {
                throw new ConfigurationException("No Flume configuration provided");
            }

            if (agents != null && agents.size() > 0) {
                props.put(name + ".sources", FlumeEmbeddedManager.SOURCE_NAME);
                props.put(name + ".sources." + FlumeEmbeddedManager.SOURCE_NAME + ".type", SOURCE_TYPE);

                if (dataDir != null && dataDir.length() > 0) {
                    if (dataDir.equals(IN_MEMORY)) {
                        props.put(name + ".channels", "primary");
                        props.put(name + ".channels.primary.type", "memory");
                    } else {
                        props.put(name + ".channels", "primary");
                        props.put(name + ".channels.primary.type", "file");

                        if (!dataDir.endsWith(FiLE_SEP)) {
                            dataDir = dataDir + FiLE_SEP;
                        }

                        props.put(name + ".channels.primary.checkpointDir", dataDir + "checkpoint");
                        props.put(name + ".channels.primary.dataDirs", dataDir + "data");
                    }
                } else {
                    props.put(name + ".channels", "primary");
                    props.put(name + ".channels.primary.type", "file");
                }

                final StringBuilder sb = new StringBuilder();
                String leading = "";
                int priority = agents.size();
                for (int i = 0; i < agents.size(); ++i) {
                    sb.append(leading).append("agent").append(i);
                    leading = " ";
                    final String prefix = name + ".sinks.agent" + i;
                    props.put(prefix + ".channel", "primary");
                    props.put(prefix + ".type", "avro");
                    props.put(prefix + ".hostname", agents.get(i).getHost());
                    props.put(prefix + ".port", Integer.toString(agents.get(i).getPort()));
                    props.put(prefix + ".batch-size", Integer.toString(batchSize));
                    props.put(name + ".sinkgroups.group1.processor.priority.agent" + i, Integer.toString(priority));
                    --priority;
                }
                props.put(name + ".sinks", sb.toString());
                props.put(name + ".sinkgroups", "group1");
                props.put(name + ".sinkgroups.group1.sinks", sb.toString());
                props.put(name + ".sinkgroups.group1.processor.type", "failover");
                final String sourceChannels = "primary";
                props.put(name + ".channels", sourceChannels);
                props.put(name + ".sources." + FlumeEmbeddedManager.SOURCE_NAME + ".channels", sourceChannels);
            }
            return props;
        }

    }

}
