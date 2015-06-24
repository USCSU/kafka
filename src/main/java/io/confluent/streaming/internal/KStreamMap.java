package io.confluent.streaming.internal;

import io.confluent.streaming.KeyValueMapper;
import io.confluent.streaming.KeyValue;

/**
 * Created by yasuhiro on 6/17/15.
 */
class KStreamMap<K, V, K1, V1> extends KStreamImpl<K, V, K1, V1> {

  private final KeyValueMapper<K, V, K1, V1> mapper;

  KStreamMap(KeyValueMapper<K, V, K1, V1> mapper, KStreamContextImpl context) {
    super(PartitioningInfo.missing, context);
    this.mapper = mapper;
  }

  public void receive(K1 key, V1 value, long timestamp) {
    synchronized (this) {
      KeyValue<K, V> newPair = mapper.apply(key, value);
      forward(newPair.key, newPair.value, timestamp);
    }
  }

}
