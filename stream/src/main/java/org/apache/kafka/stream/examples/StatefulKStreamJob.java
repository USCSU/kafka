package org.apache.kafka.stream.examples;

import io.confluent.streaming.KStreamContext;
import io.confluent.streaming.KafkaStreaming;
import io.confluent.streaming.Processor;
import io.confluent.streaming.SingleProcessorTopology;
import io.confluent.streaming.StreamingConfig;
import io.confluent.streaming.kv.Entry;
import io.confluent.streaming.kv.InMemoryKeyValueStore;
import io.confluent.streaming.kv.KeyValueIterator;
import io.confluent.streaming.kv.KeyValueStore;

import java.util.Properties;

/**
 * Created by guozhang on 7/27/15.
 */

public class StatefulKStreamJob implements Processor<String, Integer> {

  private KStreamContext context;
  private KeyValueStore<String, Integer> kvStore;

  @Override
  public void init(KStreamContext context) {
    this.context = context;
    this.context.schedule(this, 1000);

    this.kvStore = new InMemoryKeyValueStore<>("local-state", context);
  }

  @Override
  public void process(String key, Integer value) {
    Integer oldValue = this.kvStore.get(key);
    if (oldValue == null) {
      this.kvStore.put(key, value);
    } else {
      int newValue = oldValue + value;
      this.kvStore.put(key, newValue);
    }

    context.commit();
  }

  @Override
  public void punctuate(long streamTime) {
    KeyValueIterator<String, Integer> iter = this.kvStore.all();
    while (iter.hasNext()) {
      Entry<String, Integer> entry = iter.next();
      System.out.println("[" + entry.key() + ", " + entry.value() + "]");
    }
  }

  @Override
  public void close() {
    // do nothing
  }

  public static void main(String[] args) {
    KafkaStreaming streaming = new KafkaStreaming(
      new SingleProcessorTopology(StatefulKStreamJob.class, args),
      new StreamingConfig(new Properties())
    );
    streaming.run();
  }
}
