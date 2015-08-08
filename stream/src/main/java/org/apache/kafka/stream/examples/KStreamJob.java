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

import org.apache.kafka.stream.internals.KStreamConfig;
import org.apache.kafka.stream.KStreamProcess;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.stream.KStream;
import org.apache.kafka.stream.KStreamTopology;
import org.apache.kafka.stream.KeyValue;
import org.apache.kafka.stream.KeyValueMapper;
import org.apache.kafka.stream.Predicate;

import java.util.Properties;

public class KStreamJob {

    private static class MyKStreamTopology extends KStreamTopology {

        @Override
        public void build() {
            // With overridden de-serializer
            KStream<String, String> stream1 = from(new StringDeserializer(), new StringDeserializer(), "topic1");

            KStream<String, Integer> stream2 =
                stream1.map(new KeyValueMapper<String, String, String, Integer>() {
                    @Override
                    public KeyValue<String, Integer> apply(String key, String value) {
                        return new KeyValue<>(key, new Integer(value));
                    }
                }).filter(new Predicate<String, Integer>() {
                    @Override
                    public boolean apply(String key, Integer value) {
                        return true;
                    }
                });

            KStream<String, Integer>[] streams = stream2.branch(
                new Predicate<String, Integer>() {
                    @Override
                    public boolean apply(String key, Integer value) {
                        return true;
                    }
                },
                new Predicate<String, Integer>() {
                    @Override
                    public boolean apply(String key, Integer value) {
                        return true;
                    }
                }
            );

            streams[0].sendTo("topic2");
            streams[1].sendTo("topic3");
        }
    }

    public static void main(String[] args) throws Exception {
        KStreamProcess kstream = new KStreamProcess(MyKStreamTopology.class, new KStreamConfig(new Properties()));
        kstream.run();
    }
}
