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

package org.apache.kafka.stream;

import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.stream.internals.KStreamSource;
import org.apache.kafka.test.MockKStreamTopology;
import org.apache.kafka.test.MockProcessor;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;

public class KStreamFlatMapTest {

    private String topicName = "topic";

    private KStreamTopology topology = new MockKStreamTopology();
    private IntegerDeserializer keyDeserializer = new IntegerDeserializer();
    private StringDeserializer valDeserializer = new StringDeserializer();

    @Test
    public void testFlatMap() {

        KeyValueMapper<Integer, String, String, Iterable<String>> mapper =
            new KeyValueMapper<Integer, String, String, Iterable<String>>() {
                @Override
                public KeyValue<String, Iterable<String>> apply(Integer key, String value) {
                    ArrayList<String> result = new ArrayList<String>();
                    for (int i = 0; i < key; i++) {
                        result.add(value);
                    }
                    return KeyValue.pair(Integer.toString(key * 10), (Iterable<String>) result);
                }
            };

        final int[] expectedKeys = new int[]{0, 1, 2, 3};

        KStream<Integer, String> stream;
        MockProcessor<String, String> processor;

        processor = new MockProcessor<>();
        stream = topology.<Integer, String>from(keyDeserializer, valDeserializer, topicName);
        stream.flatMap(mapper).process(processor);

        for (int i = 0; i < expectedKeys.length; i++) {
            ((KStreamSource<Integer, String>) stream).source().receive(expectedKeys[i], "V" + expectedKeys[i]);
        }

        assertEquals(6, processor.processed.size());

        String[] expected = new String[]{"10:V1", "20:V2", "20:V2", "30:V3", "30:V3", "30:V3"};

        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], processor.processed.get(i));
        }
    }

}
