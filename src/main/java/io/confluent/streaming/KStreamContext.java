package io.confluent.streaming;

import io.confluent.streaming.kv.internals.RestoreFunc;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.File;
import java.util.Map;

/**
 * KStreamContext is access to the system resources for a stream processing job.
 * An instance of KStreamContext is created for each partition group.
 */
public interface KStreamContext {

  /**
   * Returns the partition group id
   * @return partition group id
   */
  int id();

  /**
   * Returns the key serializer
   * @return the key serializer
   */
  Serializer<?> keySerializer();

  /**
   * Returns the value serializer
   * @return the value serializer
   */
  Serializer<?> valueSerializer();

  /**
   * Returns the key deserializer
   * @return the key deserializer
   */
  Deserializer<?> keyDeserializer();

  /**
   * Returns the value deserializer
   * @return the value deserializer
   */
  Deserializer<?> valueDeserializer();

  /**
   * Returns a RecordCollector
   * @return RecordCollector
   */
  RecordCollector recordCollector();

  /**
   * Returns an application context registered to {@link StreamingConfig}.
   * @return an application context
   */
  Map<String, Object> getContext();

  /**
   * Returns the state directory for the partition.
   * @return the state directory
   */
  File stateDir();

  /**
   * Returns Metrics instance
   * @return Metrics
   */
  Metrics metrics();

  /**
   * Registers and possibly restores the specified storage engine.
   * @param store the storage engine
   */
  void register(StateStore store, RestoreFunc restoreFunc);

  /**
   * Flush the local state of this context
   */
  void flush();

  void send(String topic, Object key, Object value);

  void send(String topic, Object key, Object value, Serializer<Object> keySerializer, Serializer<Object> valSerializer);

  void schedule(Processor processor, long interval);

  void commit();

  String topic();

  int partition();

  long offset();

  long timestamp();

}
