package io.confluent.streaming.internal;

import io.confluent.streaming.KStreamException;
import io.confluent.streaming.Processor;
import io.confluent.streaming.PunctuationScheduler;
import io.confluent.streaming.RecordCollector;
import io.confluent.streaming.StateStore;
import io.confluent.streaming.StreamingConfig;
import io.confluent.streaming.KStreamContext;
import io.confluent.streaming.TimestampExtractor;
import io.confluent.streaming.kv.internals.RestoreFunc;
import io.confluent.streaming.util.Util;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

/**
 * Created by yasuhiro on 6/19/15.
 */
public class KStreamContextImpl implements KStreamContext {

  private static final Logger log = LoggerFactory.getLogger(KStreamContextImpl.class);

  public final int id;
  final StreamGroup streamGroup;
  final Ingestor ingestor;

  private final RecordCollectorImpl collector;
  private final HashMap<String, KStreamSource<?, ?>> sourceStreams = new HashMap<>();
  private final HashMap<String, PartitioningInfo> partitioningInfos = new HashMap<>();
  private final TimestampExtractor timestampExtractor;
  private final StreamingConfig streamingConfig;
  private final ProcessorConfig processorConfig;
  private final Metrics metrics;
  private final ProcessorStateManager stateMgr;

  private boolean initialized = false;

  @SuppressWarnings("unchecked")
  public KStreamContextImpl(int id,
                         Ingestor ingestor,
                         RecordCollectorImpl collector,
                         StreamingConfig streamingConfig,
                         ProcessorConfig processorConfig,
                         Metrics metrics) {
    this.id = id;
    this.ingestor = ingestor;
    this.collector = collector;
    this.streamingConfig = streamingConfig;
    this.processorConfig = processorConfig;
    this.timestampExtractor = this.streamingConfig.timestampExtractor();
    this.stateMgr = new ProcessorStateManager(id, new File(processorConfig.stateDir, Integer.toString(id)),
        new KafkaConsumer<>(streamingConfig.config(), null, new ByteArrayDeserializer(), new ByteArrayDeserializer()));
    this.metrics = metrics;
    this.streamGroup = new StreamGroup(this, this.ingestor, new TimeBasedChooser(), this.timestampExtractor, this.processorConfig.bufferedRecordsPerPartition);
  }

  @Override
  public int id() {
    return id;
  }

  @Override
  public Serializer<?> keySerializer() {
    return streamingConfig.keySerializer();
  }

  @Override
  public Serializer<?> valueSerializer() {
    return streamingConfig.valueSerializer();
  }

  @Override
  public Deserializer<?> keyDeserializer() {
    return streamingConfig.keyDeserializer();
  }

  @Override
  public Deserializer<?> valueDeserializer() {
    return streamingConfig.valueDeserializer();
  }

  @Override
  public RecordCollector recordCollector() {
    return collector;
  }

  @Override
  public Map<String, Object> getContext() {
    return streamingConfig.context();
  }

  @Override
  public File stateDir() {
    return stateMgr.baseDir();
  }

  public ProcessorStateManager stateMgr() {
    return stateMgr;
  }

  @Override
  public Metrics metrics() {
    return metrics;
  }

  @Override
  public void restore(StateStore store, RestoreFunc restoreFunc) {
    ensureInitialization();

    stateMgr.registerAndRestore(store, restoreFunc);
  }

  public void ensureInitialization() {
    if (!initialized)
      throw new IllegalStateException("context initialization is already finished");
  }

  @Override
  public void flush() {
    stateMgr.flush();
  }

  @Override
  public String topic() {
    if (this.streamGroup.record() == null)
      throw new IllegalStateException("this should not happen as topic() should only be called while a record is processed");

    return this.streamGroup.record().topic();
  }

  @Override
  public int partition() {
    if (this.streamGroup.record() == null)
      throw new IllegalStateException("this should not happen as partition() should only be called while a record is processed");

    return this.streamGroup.record().partition();
  }

  @Override
  public long offset() {
    if (this.streamGroup.record() == null)
      throw new IllegalStateException("this should not happen as offset() should only be called while a record is processed");

    return this.streamGroup.record().offset();
  }

  @Override
  public long timestamp() {
    if (this.streamGroup.record() == null)
      throw new IllegalStateException("this should not happen as timestamp() should only be called while a record is processed");

    return this.streamGroup.record().timestamp;
  }

  @Override
  public void send(String topic, Object key, Object value) {
    collector.send(new ProducerRecord<>(topic, key, value));
  }

  @Override
  public void send(String topic, Object key, Object value, Serializer<Object> keySerializer, Serializer<Object> valSerializer) {
    if (keySerializer == null || valSerializer == null)
      throw new IllegalStateException("key and value serializers must be specified");

    collector.send(new ProducerRecord<>(topic, key, value), keySerializer, valSerializer);
  }

  @Override
  public void commit() {
    this.streamGroup.commitOffset();
  }

  @Override
  public PunctuationScheduler getPunctuationScheduler(Processor processor) {
    return streamGroup.getPunctuationScheduler(processor);
  }

  public void init(Collection<KStreamSource<?, ?>> streams) throws IOException {
    stateMgr.init();

    for (KStreamSource stream: streams) {
      KStreamMetadata metadata = linkStreamToTopics(stream);

      stream.bind(this, metadata);
    }

    // add partition -> stream group mappings to the ingestor
    for (Map.Entry<String, KStreamSource<?,?>> entry : sourceStreams.entrySet()) {
      TopicPartition partition = new TopicPartition(entry.getKey(), id);
      ingestor.addPartitionStreamToGroup(streamGroup, partition);
    }

    if (!ingestor.topics().equals(sourceStreams.keySet())) {
      LinkedList<String> unusedTopics = new LinkedList<>();
      for (String topic : ingestor.topics()) {
        if (!sourceStreams.containsKey(topic))
          unusedTopics.add(topic);
      }
      throw new KStreamException("unused topics: " + Util.mkString(unusedTopics));
    }

    initialized = true;
  }

  private KStreamMetadata linkStreamToTopics(KStreamSource stream) {
    ensureInitialization();

    Set<String> fromTopics;

    synchronized (this) {
      // if topics not specified, use all the topics be default
      if (stream.topics == null || stream.topics.length == 0) {
        fromTopics = ingestor.topics();
      } else {
        fromTopics = Collections.unmodifiableSet(Util.mkSet(stream.topics));
      }

      // iterate over the topics and check if the stream has already been created for them
      for (String topic : fromTopics) {
        if (!ingestor.topics().contains(topic))
          throw new IllegalArgumentException("topic not subscribed: " + topic);

        if (sourceStreams.containsKey(topic))
          throw new IllegalArgumentException("another stream created with the same topic " + topic);
      }

      // create stream metadata
      Map<String, PartitioningInfo> topicPartitionInfos = new HashMap<>();
      for (String topic : fromTopics) {
        PartitioningInfo partitioningInfo = this.partitioningInfos.get(topic);

        if (partitioningInfo == null) {
          partitioningInfo = new PartitioningInfo(ingestor.numPartitions(topic));
          this.partitioningInfos.put(topic, partitioningInfo);
        }

        topicPartitionInfos.put(topic, partitioningInfo);
      }

      // update source stream map
      for (String topic : fromTopics) {
        sourceStreams.put(topic, stream);

        TopicPartition partition = new TopicPartition(topic, id);
        streamGroup.addPartition(partition, stream);
      }

      return new KStreamMetadata(topicPartitionInfos);
    }
  }

  public Map<TopicPartition, Long> consumedOffsets() {
    return streamGroup.consumedOffsets();
  }

  public void close() throws Exception {
    stateMgr.close(collector.offsets());
    streamGroup.close();
  }

}
