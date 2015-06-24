package io.confluent.streaming.internal;

/**
 * Created by yasuhiro on 6/17/15.
 */
interface Receiver<K,V> {

  void receive(K key, V value, long timestamp);

  void punctuate(long timestamp);

  void flush();

}
