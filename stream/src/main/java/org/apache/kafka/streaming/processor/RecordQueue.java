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

package org.apache.kafka.streaming.processor;

import org.apache.kafka.common.TopicPartition;

/**
 * RecordQueue is a queue of {@link StampedRecord} (ConsumerRecord + timestamp).
 */
public interface RecordQueue {

    /**
     * Returns the partition with which this queue is associated
     *
     * @return TopicPartition
     */
    TopicPartition partition();

    /**
     * Returns the corresponding source processor with this queue
     *
     * @return KafkaProcessor
     */
    KafkaProcessor source();

    /**
     * Adds a StampedRecord to the queue
     *
     * @param record StampedRecord
     */
    void add(StampedRecord record);

    /**
     * Returns the next record fro the queue
     *
     * @return StampedRecord
     */
    StampedRecord next();

    /**
     * Returns the highest offset in the queue
     *
     * @return offset
     */
    long offset();

    /**
     * Returns the number of records in the queue
     *
     * @return the number of records
     */
    int size();

    /**
     * Tests if the queue is empty
     *
     * @return true if the queue is empty, otherwise false
     */
    boolean isEmpty();

    /**
     * Returns a timestamp tracked by the TimestampTracker
     *
     * @return timestamp
     */
    long trackedTimestamp();

}
