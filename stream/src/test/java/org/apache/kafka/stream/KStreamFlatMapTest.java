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

import org.apache.kafka.stream.internals.PartitioningInfo;
import org.apache.kafka.stream.topology.KStreamTopology;
import org.apache.kafka.stream.topology.KeyValue;
import org.apache.kafka.stream.topology.KeyValueMapper;
import org.apache.kafka.stream.topology.internals.KStreamMetadata;
import org.apache.kafka.stream.topology.internals.KStreamSource;
import org.apache.kafka.test.MockKStreamTopology;
import org.apache.kafka.test.MockProcessor;
import org.apache.kafka.test.MockKStreamContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;

public class KStreamFlatMapTest {

    private String topicName = "topic";

    private KStreamMetadata streamMetadata = new KStreamMetadata(Collections.singletonMap(topicName, new PartitioningInfo(1)));

    @Test
    public void testFlatMap() {

        KeyValueMapper<String, Iterable<String>, Integer, String> mapper =
            new KeyValueMapper<String, Iterable<String>, Integer, String>() {
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

        KStreamTopology topology = new MockKStreamTopology();

        KStreamSource<Integer, String> stream;
        MockProcessor<String, String> processor;

        processor = new MockProcessor<>();
        stream = new KStreamSource<>(null, topology);
        stream.flatMap(mapper).process(processor);

        KStreamContext context = new MockKStreamContext(null, null);
        stream.bind(context, streamMetadata);
        for (int i = 0; i < expectedKeys.length; i++) {
            stream.receive(expectedKeys[i], "V" + expectedKeys[i], 0L);
        }

        assertEquals(6, processor.processed.size());

        String[] expected = new String[]{"10:V1", "20:V2", "20:V2", "30:V3", "30:V3", "30:V3"};

        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], processor.processed.get(i));
        }
    }

}
