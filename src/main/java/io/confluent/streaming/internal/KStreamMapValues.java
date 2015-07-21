package io.confluent.streaming.internal;

import io.confluent.streaming.KStreamContext;
import io.confluent.streaming.ValueMapper;

/**
 * Created by yasuhiro on 6/17/15.
 */
class KStreamMapValues<K, V, V1> extends KStreamImpl<K, V> {

  private final ValueMapper<V, V1> mapper;

  KStreamMapValues(ValueMapper<V, V1> mapper, KStreamMetadata metadata, KStreamContext context) {
    super(metadata, context);
    this.mapper = mapper;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void receive(String topic, Object key, Object value, long timestamp, long streamTime) {
    synchronized (this) {
      V newValue = mapper.apply((V1)value);
      forward(KStreamMetadata.UNKNOWN_TOPICNAME, key, newValue, timestamp, streamTime);
    }
  }

}
