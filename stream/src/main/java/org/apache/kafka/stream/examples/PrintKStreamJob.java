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

import org.apache.kafka.stream.KStreamContext;
import org.apache.kafka.stream.KafkaStreaming;
import org.apache.kafka.stream.StreamingConfig;
import org.apache.kafka.stream.topology.KStreamTopology;
import org.apache.kafka.stream.topology.Processor;

import java.util.Properties;

public class PrintKStreamJob extends KStreamTopology {

    private class MyProcessor<K, V> implements Processor<K, V> {
        private KStreamContext context;

        @Override
        public void init(KStreamContext context) {
            this.context = context;
        }

        @Override
        public void process(K key, V value) {
            System.out.println("[" + key + ", " + value + "]");

            context.commit();

            context.send("topic", key, value);
        }

        @Override
        public void punctuate(long streamTime) {
            // do nothing
        }

        @Override
        public void close() {
            // do nothing
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void topology() { from("topic").process(new MyProcessor()); }

    public static void main(String[] args) {
        KafkaStreaming streaming = new KafkaStreaming(
            new PrintKStreamJob(),
            new StreamingConfig(new Properties())
        );
        streaming.run();
    }
}
