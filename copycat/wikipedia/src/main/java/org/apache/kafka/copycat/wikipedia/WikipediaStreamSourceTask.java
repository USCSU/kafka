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

import org.apache.kafka.copycat.data.Schema;
import org.apache.kafka.copycat.source.SourceRecord;
import org.apache.kafka.copycat.wikipedia.WikipediaFeed.WikipediaFeedEvent;
import org.apache.kafka.copycat.wikipedia.WikipediaFeed.WikipediaFeedListener;
import org.apache.kafka.copycat.errors.CopycatException;
import org.apache.kafka.copycat.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;


public class WikipediaStreamSourceTask extends SourceTask implements WikipediaFeedListener {

    private static final Logger log = LoggerFactory.getLogger(WikipediaStreamSourceTask.class);
    private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;
    private static final String CHANNEL_FIELD = "channel";

    WikipediaFeed feed;
    private final List<String> channels = new ArrayList<>();
    private final List<SourceRecord> records = new ArrayList<>();

    private String topic = null;

    // synchronize the callback and the poll call that access the records buffer
    @Override
    public synchronized void onEvent(final WikipediaFeedEvent event) {
        try {
            records.add(new SourceRecord(offsetKey(event.getChannel()), null, topic, VALUE_SCHEMA, WikipediaFeedEvent.toJson(event)));
        } catch (Exception e) {
            System.err.println(e);
        }
    }


    @Override
    public void start(Properties props) {
        log.trace("Starting");

        String host = props.getProperty(WikipediaStreamSourceConnector.HOST_CONFIG);
        if (host == null || host.isEmpty()) {
            throw new CopycatException("Host is not specified for WikipediaSourceTask");
        }

        int port = Integer.parseInt(props.getProperty(WikipediaStreamSourceConnector.PORT_CONFIG));

        topic = props.getProperty(WikipediaStreamSourceConnector.TOPIC_CONFIG);

        if (topic == null)
            throw new CopycatException("WikipediaSourceTask config missing topic setting");

        feed = new WikipediaFeed(host, port);
        feed.start();

        String channelsValue = props.getProperty(WikipediaStreamSourceConnector.CHANNELS_CONFIG);
        if (channelsValue == null || channelsValue.isEmpty()) {
            throw new CopycatException("Channel names are not specified for WikipediaSourceTask");
        }
        // register all channels at once
        for (String channel : channelsValue.split(WikipediaStreamSourceConnector.CHANNEL_SEPARATOR)) {
            channels.add(channel);
            feed.listen(channel, this);
        }
    }

    @Override
    public synchronized List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> ret = new ArrayList<>();
        ret.addAll(records);
        records.clear();

        return ret;
    }

    @Override
    public void stop() {
        log.trace("Stopping");
        synchronized (this) {
            for (String channel : channels) {
                feed.unlisten(channel, this);
            }

            feed.stop();

            this.notify();
        }
    }

    private Map<String, String> offsetKey(String channel) {
        return Collections.singletonMap(CHANNEL_FIELD, channel);
    }
}
