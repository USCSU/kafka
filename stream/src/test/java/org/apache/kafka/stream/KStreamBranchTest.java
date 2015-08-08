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

import org.apache.kafka.clients.processor.internals.PartitioningInfo;
import org.apache.kafka.stream.internals.KStreamMetadata;
import org.apache.kafka.stream.internals.KStreamSource;
import org.apache.kafka.test.MockKStreamContext;
import org.apache.kafka.test.MockKStreamTopology;
import org.apache.kafka.test.MockProcessor;
import org.junit.Test;

import java.lang.reflect.Array;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class KStreamBranchTest {

    private String topicName = "topic";

    private KStreamMetadata streamMetadata = new KStreamMetadata(Collections.singletonMap(topicName, new PartitioningInfo(1)));

    @SuppressWarnings("unchecked")
    @Test
    public void testKStreamBranch() {

        Predicate<Integer, String> isEven = new Predicate<Integer, String>() {
            @Override
            public boolean apply(Integer key, String value) {
                return (key % 2) == 0;
            }
        };
        Predicate<Integer, String> isMultipleOfThree = new Predicate<Integer, String>() {
            @Override
            public boolean apply(Integer key, String value) {
                return (key % 3) == 0;
            }
        };
        Predicate<Integer, String> isOdd = new Predicate<Integer, String>() {
            @Override
            public boolean apply(Integer key, String value) {
                return (key % 2) != 0;
            }
        };

        final int[] expectedKeys = new int[]{1, 2, 3, 4, 5, 6, 7};

        KStreamTopology initializer = new MockKStreamTopology();
        KStreamSource<Integer, String> stream;
        KStream<Integer, String>[] branches;
        MockProcessor<Integer, String>[] processors;

        stream = new KStreamSource<>(null, initializer);
        branches = stream.branch(isEven, isMultipleOfThree, isOdd);

        assertEquals(3, branches.length);

        processors = (MockProcessor<Integer, String>[]) Array.newInstance(MockProcessor.class, branches.length);
        for (int i = 0; i < branches.length; i++) {
            processors[i] = new MockProcessor<>();
            branches[i].process(processors[i]);
        }

        for (int i = 0; i < expectedKeys.length; i++) {
            stream.receive(expectedKeys[i], "V" + expectedKeys[i], 0L);
        }

        assertEquals(3, processors[0].processed.size());
        assertEquals(1, processors[1].processed.size());
        assertEquals(3, processors[2].processed.size());

        stream = new KStreamSource<>(null, initializer);
        branches = stream.branch(isEven, isOdd, isMultipleOfThree);

        assertEquals(3, branches.length);

        processors = (MockProcessor<Integer, String>[]) Array.newInstance(MockProcessor.class, branches.length);
        for (int i = 0; i < branches.length; i++) {
            processors[i] = new MockProcessor<>();
            branches[i].process(processors[i]);
        }

        KStreamContext context = new MockKStreamContext(null, null);
        stream.bind(context, streamMetadata);
        for (int i = 0; i < expectedKeys.length; i++) {
            stream.receive(expectedKeys[i], "V" + expectedKeys[i], 0L);
        }

        assertEquals(3, processors[0].processed.size());
        assertEquals(4, processors[1].processed.size());
        assertEquals(0, processors[2].processed.size());
    }

}
