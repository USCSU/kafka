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
 */

package org.apache.kafka.stream.examples;

import org.apache.kafka.stream.KStream;
import org.apache.kafka.stream.KStreamContext;
import org.apache.kafka.stream.KafkaStreaming;
import org.apache.kafka.stream.StreamingConfig;
import org.apache.kafka.stream.state.Entry;
import org.apache.kafka.stream.state.InMemoryKeyValueStore;
import org.apache.kafka.stream.state.KeyValueIterator;
import org.apache.kafka.stream.state.KeyValueStore;
import org.apache.kafka.stream.topology.KStreamTopology;
import org.apache.kafka.stream.topology.Processor;

import java.util.Properties;

public class StatefulKStreamJob extends KStreamTopology {

    private class MyProcessor implements Processor<String, Integer> {
        private KStreamContext context;
        private KeyValueStore<String, Integer> kvStore;

        @Override
        public void init(KStreamContext context) {
            this.context = context;
            this.context.schedule(this, 1000);

            this.kvStore = new InMemoryKeyValueStore<>("local-state", context);
        }

        @Override
        public void process(String key, Integer value) {
            Integer oldValue = this.kvStore.get(key);
            if (oldValue == null) {
                this.kvStore.put(key, value);
            } else {
                int newValue = oldValue + value;
                this.kvStore.put(key, newValue);
            }

            context.commit();
        }

        @Override
        public void punctuate(long streamTime) {
            KeyValueIterator<String, Integer> iter = this.kvStore.all();
            while (iter.hasNext()) {
                Entry<String, Integer> entry = iter.next();
                System.out.println("[" + entry.key() + ", " + entry.value() + "]");
            }
        }

        @Override
        public void close() {
            // do nothing
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void build() { ((KStream<String, Integer>) from("topic")).process(new MyProcessor()); }

    public static void main(String[] args) {
        KafkaStreaming streaming = new KafkaStreaming(
            new StatefulKStreamJob(),
            new StreamingConfig(new Properties())
        );
        streaming.run();
    }
}
