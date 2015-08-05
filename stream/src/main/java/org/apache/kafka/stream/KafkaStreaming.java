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

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.stream.internal.ProcessorConfig;
import org.apache.kafka.stream.topology.KStreamTopology;
import org.apache.kafka.stream.topology.internal.KStreamThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * Kafka Streaming allows for performing continuous computation on input coming from one or more input topics and
 * sends output to zero or more output topics.
 * <p>
 * This processing is defined by extending the {@link KStreamTopology} abstract class to specify the transformation operator topology. The
 * {@link KafkaStreaming} instance will be responsible for the lifecycle of these processors. It will instantiate and
 * start one or more of these processors to process the Kafka partitions assigned to this particular instance.
 * <p>
 * This streaming instance will co-ordinate with any other instances (whether in this same process, on other processes
 * on this machine, or on remote machines). These processes will divide up the work so that all partitions are being
 * consumed. If instances are added or die, the corresponding {@link KStream} instances will be shutdown or
 * started in the appropriate processes to balance processing load.
 * <p>
 * Internally the {@link KafkaStreaming} instance contains a normal {@link org.apache.kafka.clients.producer.KafkaProducer KafkaProducer}
 * and {@link org.apache.kafka.clients.consumer.KafkaConsumer KafkaConsumer} instance that is used for reading input and writing output.
 * <p>
 * A simple example might look like this:
 * <pre>
 *    Properties props = new Properties();
 *    props.put("bootstrap.servers", "localhost:4242");
 *    StreamingConfig config = new StreamingConfig(props);
 *    config.processor(ExampleStreamProcessor.class);
 *    config.serialization(new StringSerializer(), new StringDeserializer());
 *    KafkaStreaming container = new KafkaStreaming(new MyKStreamTopology(), config);
 *    container.run();
 * </pre>
 *
 */
public class KafkaStreaming implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(KafkaStreaming.class);

    // Container States
    private final int CREATED = 0;
    private final int RUNNING = 1;
    private final int STOPPING = 2;
    private final int STOPPED = 3;
    private int state = CREATED;

    private final ProcessorConfig config;
    private final Object lock = new Object();
    private final KStreamThread[] threads;
    private final Set<String> topics;


    public KafkaStreaming(KStreamTopology topology, StreamingConfig streamingConfig) {

        if (streamingConfig.timestampExtractor() == null) throw new NullPointerException("timestamp extractor is missing");

        this.config = new ProcessorConfig(streamingConfig.config());
        this.topics = topology.topics();

        Metrics metrics = new Metrics();

        // TODO: Fix this after the threading model is decided (also fix KStreamThread)
        this.threads = new KStreamThread[1];
        threads[0] = new KStreamThread(topology, topics, streamingConfig, metrics);
    }

    /**
     * Execute the stream processors
     */
    public void run() {
        synchronized (lock) {
            log.info("Starting container");
            if (state == CREATED) {
                if (!config.stateDir.exists() && !config.stateDir.mkdirs())
                    throw new IllegalArgumentException("Failed to create state directory: " + config.stateDir.getAbsolutePath());

                for (KStreamThread thread : threads) thread.start();
                log.info("Start-up complete");
            } else {
                throw new IllegalStateException("This container was already started");
            }

            state = RUNNING;
            while (state == RUNNING) {
                try {
                    lock.wait();
                }
                catch (InterruptedException ex) {
                    Thread.interrupted();
                }
            }

            if (state == STOPPING) {
                log.info("Shutting down the container");

                for (KStreamThread thread : threads)
                    thread.close();

                for (KStreamThread thread : threads) {
                    try {
                        thread.join();
                    } catch (InterruptedException ex) {
                        Thread.interrupted();
                    }
                }
                state = STOPPED;
                lock.notifyAll();
                log.info("Shutdown complete");
            }
        }
    }

    /**
     * Shutdown this streaming instance.
     */
    public void close() {
        synchronized (lock) {
            if (state == CREATED || state == RUNNING) {
                state = STOPPING;
                lock.notifyAll();
            }
            while (state == STOPPING) {
                try {
                    lock.wait();
                }
                catch (InterruptedException ex) {
                    Thread.interrupted();
                }
            }
        }
    }

}
