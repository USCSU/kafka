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

package io.confluent.streaming;

import io.confluent.streaming.internal.KStreamContextImpl;
import io.confluent.streaming.internal.ProcessorConfig;
import io.confluent.streaming.internal.IngestorImpl;
import io.confluent.streaming.internal.StreamGroup;
import io.confluent.streaming.util.ParallelExecutor;
import io.confluent.streaming.util.Util;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Kafka Streaming allows for performing continuous computation on input coming from one or more input topics and
 * sends output to zero or more output topics.
 * <p>
 * This processing is done by implementing the {@link KStreamJob} interface to specify the transformation. The
 * {@link KafkaStreaming} instance will be responsible for the lifecycle of these processors. It will instantiate and
 * start one or more of these processors to process the Kafka partitions assigned to this particular instance.
 * <p>
 * This streaming instance will co-ordinate with any other instances (whether in this same process, on other processes
 * on this machine, or on remote machines). These processes will divide up the work so that all partitions are being
 * consumed. If instances are added or die, the corresponding {@link KStreamJob} instances will be shutdown or
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
 *    KafkaStreaming container = new KafkaStreaming(MyKStreamJob.class, config);
 *    container.run();
 * </pre>
 *
 */
public class KafkaStreaming implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(KafkaStreaming.class);

    private final Class<? extends KStreamJob> jobClass;
    private final Set<String> topics;
    private final ArrayList<StreamGroup> streamGroups = new ArrayList<>();
    private final ParallelExecutor parallelExecutor;
    private final Map<Integer, KStreamContextImpl> kstreamContexts = new HashMap<>();
    protected final Producer<byte[], byte[]> producer;
    protected final Consumer<byte[], byte[]> consumer;
    private final IngestorImpl ingestor;
    private final StreamingConfig streamingConfig;
    private final ProcessorConfig config;
    private final Metrics metrics;
    private final KafkaStreamingMetrics streamingMetrics;
    private final Time time;
    private volatile boolean requestingCommit = false;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private volatile boolean running;
    private CountDownLatch shutdownComplete = new CountDownLatch(1);
    private long lastCommit;
    private long nextStateCleaning;
    private long recordsProcessed;

    protected final ConsumerRebalanceCallback rebalanceCallback = new ConsumerRebalanceCallback() {
        @Override
        public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> assignment) {
            addPartitions(assignment);
        }

        @Override
        public void onPartitionsRevoked(Consumer<?, ?> consumer, Collection<TopicPartition> assignment) {
            removePartitions(assignment);
        }
    };

    public KafkaStreaming(Class<? extends KStreamJob> jobClass, StreamingConfig config) {
        this(jobClass, config, null, null);
    }

    @SuppressWarnings("unchecked")
    protected KafkaStreaming(Class<? extends KStreamJob> jobClass,
                             StreamingConfig config,
                             Producer<byte[], byte[]> producer,
                             Consumer<byte[], byte[]> consumer) {
        this.jobClass = jobClass;
        this.producer = producer == null? new KafkaProducer<>(config.config(), new ByteArraySerializer(), new ByteArraySerializer()): producer;
        this.consumer = consumer == null? new KafkaConsumer<>(config.config(), rebalanceCallback, new ByteArrayDeserializer(), new ByteArrayDeserializer()): consumer;
        this.streamingConfig = config;
        this.metrics = new Metrics();
        this.streamingMetrics = new KafkaStreamingMetrics();
        this.config = new ProcessorConfig(config.config());
        this.ingestor = new IngestorImpl(this.consumer);
        this.running = true;
        this.lastCommit = 0;
        this.nextStateCleaning = Long.MAX_VALUE;
        this.recordsProcessed = 0;
        this.time = new SystemTime();
        this.parallelExecutor = new ParallelExecutor(this.config.numStreamThreads);

        try {
            this.topics = new HashSet<>(Arrays.asList(this.config.topics.split(",")));
        }
        catch (Exception e) {
            throw new KStreamException("failed to get a topic list from the job", e);
        }
    }

    /**
     * Execute the stream processors
     */
    public synchronized void run() {
        init();
        try {
            runLoop();
        } catch (RuntimeException e) {
            log.error("Uncaught error during processing: ", e);
            throw e;
        } finally {
            shutdown();
        }
    }

    private void init() {
        log.info("Starting container");
        if (started.compareAndSet(false, true)) {
            if (!config.stateDir.exists() && !config.stateDir.mkdirs())
                throw new IllegalArgumentException("Failed to create state directory: " + config.stateDir.getAbsolutePath());

            for (String topic : topics)
                consumer.subscribe(topic);

            log.info("Start-up complete");
        } else {
            throw new IllegalStateException("This container was already started");
        }
    }

    private void shutdown() {
        log.info("Shutting down container");
        commitAll(time.milliseconds());

        for (StreamGroup streamGroup : streamGroups) {
            try {
                streamGroup.close();
            }
            catch(Exception e) {
                log.error("Error while closing stream synchronizers: ", e);
            }
        }

        producer.close();
        consumer.close();
        parallelExecutor.shutdown();
        streamGroups.clear();
        shutdownComplete.countDown();
        log.info("Shut down complete");
    }

    /**
     * Shutdown this streaming instance.
     */
    public synchronized void close() {
        running = false;
        try {
            shutdownComplete.await();
        } catch (InterruptedException e) {
            throw new InterruptException(e);
        }
    }

    private void runLoop() {
        try {
            boolean readyForNextExecution = false;

            while (stillRunning()) {
                ingestor.poll(readyForNextExecution ? 0 : this.config.pollTimeMs);

                readyForNextExecution = parallelExecutor.execute(streamGroups);

                maybeCommit();
                maybeCleanState();
            }
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }

    private boolean stillRunning() {
        if(!running) {
            log.debug("Shutting down at user request.");
            return false;
        }
        if(config.totalRecordsToProcess >= 0 && recordsProcessed >= config.totalRecordsToProcess) {
            log.debug("Shutting down as we've reached the user-configured limit of {} records to process.", config.totalRecordsToProcess);
            return false;
        }
        return true;
    }

    private void maybeCommit() {
        long now = time.milliseconds();
        if (config.commitTimeMs >= 0 && lastCommit + config.commitTimeMs < time.milliseconds()) {
            log.trace("Committing processor instances because the commit interval has elapsed.");
            commitAll(now);
        } else if (requestingCommit) {
            requestingCommit = false;
            log.trace("Committing processor instances because of user request.");
            commitAll(now);
        }
    }

    private void commitAll(long now) {
        Map<TopicPartition, Long> commit = new HashMap<>();
        for (KStreamContextImpl context : kstreamContexts.values()) {
            context.flush();
            // check co-ordinator
        }
        for (StreamGroup streamGroup : streamGroups) {
            try {
                commit.putAll(streamGroup.consumedOffsets());
            }
            catch(Exception e) {
                log.error("Error while closing processor: ", e);
            }
        }

        // check if commit is really needed, i.e. if all the offsets are already committed
        boolean commitNeeded = false;
        for (TopicPartition tp : commit.keySet()) {
            if (consumer.committed(tp) != commit.get(tp)) {
                commitNeeded = true;
                break;
            }
        }

        if (commitNeeded) {
            // TODO: for exactly-once we need to make sure the flush and commit
            // are executed atomically whenever it is triggered by user
            producer.flush();
            consumer.commit(commit, CommitType.SYNC); // TODO: can this be async?
            streamingMetrics.commitTime.record(time.milliseconds() - lastCommit);
        }
    }

    /* delete any state dirs that aren't for active contexts */
    private void maybeCleanState() {
        long now = time.milliseconds();
        if(now > nextStateCleaning) {
            File[] stateDirs = config.stateDir.listFiles();
            if(stateDirs != null) {
                for(File dir: stateDirs) {
                    try {
                        Integer id = Integer.parseInt(dir.getName());
                        if(!kstreamContexts.keySet().contains(id)) {
                            log.info("Deleting obsolete state directory {} after {} delay ms.", dir.getAbsolutePath(), config.stateCleanupDelay);
                            Util.rm(dir);
                        }
                    } catch(NumberFormatException e) {
                        log.warn("Deleting unknown directory in state directory {}.", dir.getAbsolutePath());
                        Util.rm(dir);
                    }
                }
            }
            nextStateCleaning = Long.MAX_VALUE;
        }
    }

    private void addPartitions(Collection<TopicPartition> assignment) {
        HashSet<TopicPartition> partitions = new HashSet<>(assignment);

        ingestor.init();

        Consumer<byte[], byte[]> restoreConsumer =
          new KafkaConsumer<>(streamingConfig.config(), null, new ByteArrayDeserializer(), new ByteArrayDeserializer());

        for (TopicPartition partition : partitions) {
            final Integer id = partition.partition();
            KStreamContextImpl kstreamContext = kstreamContexts.get(id);
            if (kstreamContext == null) {
                KStreamJob job = (KStreamJob) Utils.newInstance(jobClass);

                Coordinator coordinator = new Coordinator() {
                    @Override
                    public void commit() {
                        requestingCommit = true;
                    }

                    @Override
                    public void shutdown() {
                        running = true;
                    }
                };

                kstreamContext =
                  new KStreamContextImpl(id, job, topics, ingestor, producer, coordinator, streamingConfig, config, metrics);

                kstreamContexts.put(id, kstreamContext);

                try {
                    kstreamContext.init(restoreConsumer);
                }
                catch (Exception e) {
                    throw new KafkaException(e);
                }

                Collection<StreamGroup> streamGroups = kstreamContext.streamSynchronizers();
                for (StreamGroup streamGroup : streamGroups) {
                    streamGroups.add(streamGroup);
                }
            }
        }

        restoreConsumer.close();
        nextStateCleaning = time.milliseconds() + config.stateCleanupDelay;
    }

    private void removePartitions(Collection<TopicPartition> assignment) {
        commitAll(time.milliseconds());
        for (StreamGroup streamGroup : streamGroups) {
            log.info("Removing synchronization groups {}", streamGroup.name());
            streamGroup.close();
        }
        for (KStreamContextImpl kstreamContext : kstreamContexts.values()) {
            log.info("Removing stream context {}", kstreamContext.id());
            try {
                kstreamContext.close();
            }
            catch (Exception e) {
                throw new KafkaException(e);
            }
            streamingMetrics.processorDestruction.record();
        }
        streamGroups.clear();
        ingestor.clear();
    }

    private static Set<String> extractTopics(Class<? extends KStreamJob> jobClass) {
        // extract topics from a jobClass's static member field, topics
        try {
            Object instance = Utils.newInstance(jobClass);
            return ((Topics)instance).topics;
        }
        catch (Exception e) {
            throw new KStreamException("failed to get a topic list from the job", e);
        }
    }

    private class KafkaStreamingMetrics {
        final Sensor commitTime;
        final Sensor processTime;
        final Sensor windowTime;
        final Sensor processorCreation;
        final Sensor processorDestruction;

        public KafkaStreamingMetrics() {
            String group = "kafka-streaming";

            this.commitTime = metrics.sensor("commit-time");
            this.commitTime.add(new MetricName(group, "commit-time-avg-ms"), new Avg());
            this.commitTime.add(new MetricName(group, "commits-time-max-ms"), new Max());
            this.commitTime.add(new MetricName(group, "commits-per-second"), new Rate(new Count()));

            this.processTime = metrics.sensor("process-time");
            this.commitTime.add(new MetricName(group, "process-time-avg-ms"), new Avg());
            this.commitTime.add(new MetricName(group, "process-time-max-ms"), new Max());
            this.commitTime.add(new MetricName(group, "process-calls-per-second"), new Rate(new Count()));

            this.windowTime = metrics.sensor("window-time");
            this.windowTime.add(new MetricName(group, "window-time-avg-ms"), new Avg());
            this.windowTime.add(new MetricName(group, "window-time-max-ms"), new Max());
            this.windowTime.add(new MetricName(group, "window-calls-per-second"), new Rate(new Count()));

            this.processorCreation = metrics.sensor("processor-creation");
            this.processorCreation.add(new MetricName(group, "processor-creation"), new Rate(new Count()));

            this.processorDestruction = metrics.sensor("processor-destruction");
            this.processorDestruction.add(new MetricName(group, "processor-destruction"), new Rate(new Count()));

        }

    }

}
