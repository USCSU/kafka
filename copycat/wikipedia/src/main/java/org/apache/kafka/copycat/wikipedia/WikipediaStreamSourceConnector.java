/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.copycat.wikipedia;

import org.apache.kafka.copycat.connector.Task;
import org.apache.kafka.copycat.errors.CopycatException;
import org.apache.kafka.copycat.source.SourceConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Very simple connector that support sourcing from wikipedia irc channel.
 */
public class WikipediaStreamSourceConnector extends SourceConnector {
    public static final String TOPIC_CONFIG = "topic";
    public static final String HOST_CONFIG = "host";
    public static final String PORT_CONFIG = "port";
    public static final String CHANNELS_CONFIG = "channels";
    public static final String CHANNEL_SEPARATOR = ",";

    private String host;
    private String port;
    private String topic;
    private String channels;

    @Override
    public void start(Properties props) {
        host = props.getProperty(HOST_CONFIG);
        port = props.getProperty(PORT_CONFIG);
        channels = props.getProperty(CHANNELS_CONFIG);
        topic = props.getProperty(TOPIC_CONFIG);
        if (topic == null || topic.isEmpty())
            throw new CopycatException("FileStreamSourceConnector configuration must include 'topic' setting");
        if (topic.contains(","))
            throw new CopycatException("FileStreamSourceConnector should only have a single topic when used as a source.");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return WikipediaStreamSourceTask.class;
    }

    @Override
    public List<Properties> taskConfigs(int maxTasks) {
        ArrayList<Properties> configs = new ArrayList<>();
        // Only one input stream makes sense.
        Properties config = new Properties();
        if (host != null)
            config.setProperty(HOST_CONFIG, host);
        if (port != null)
            config.setProperty(PORT_CONFIG, port);
        if (channels != null)
            config.setProperty(CHANNELS_CONFIG, channels);
        config.setProperty(TOPIC_CONFIG, topic);
        configs.add(config);
        return configs;
    }

    @Override
    public void stop() {
        // Nothing to do since WikipediaStreamSourceConnector has no background monitoring.
    }

}
