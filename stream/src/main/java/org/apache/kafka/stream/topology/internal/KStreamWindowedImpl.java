package org.apache.kafka.stream.topology.internal;


import org.apache.kafka.clients.processor.ProcessorContext;
import org.apache.kafka.stream.KStream;
import org.apache.kafka.stream.topology.KStreamTopology;
import org.apache.kafka.stream.topology.KStreamWindowed;
import org.apache.kafka.stream.topology.ValueJoiner;
import org.apache.kafka.stream.topology.Window;

/**
 * Created by yasuhiro on 6/18/15.
 */
public class KStreamWindowedImpl<K, V> extends KStreamImpl<K, V> implements KStreamWindowed<K, V> {

  final Window<K, V> window;

  KStreamWindowedImpl(Window<K, V> window, KStreamTopology initializer) {
    super(initializer);
    this.window = window;
  }

  @Override
  public void bind(ProcessorContext context, KStreamMetadata metadata) {
    super.bind(context, metadata);
    window.init(context);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void receive(Object key, Object value, long timestamp) {
    synchronized(this) {
      window.put((K)key, (V)value, timestamp);
      // KStreamWindowed needs to forward the topic name since it may receive directly from KStreamSource
      forward(key, value, timestamp);
    }
  }

  @Override
  public void close() {
    window.close();
    super.close();
  }

  @Override
  public <V1, V2> KStream<K, V2> join(KStreamWindowed<K, V1> other, ValueJoiner<V2, V, V1> processor) {
    return join(other, false, processor);
  }

  @Override
  public <V1, V2> KStream<K, V2> joinPrior(KStreamWindowed<K, V1> other, ValueJoiner<V2, V, V1> processor) {
    return join(other, true, processor);
  }

  private <V1, V2> KStream<K, V2> join(KStreamWindowed<K, V1> other, boolean prior, ValueJoiner<V2, V, V1> processor) {

    KStreamWindowedImpl<K, V1> otherImpl = (KStreamWindowedImpl<K, V1>) other;

    KStreamJoin<K, V2, V, V1> stream = new KStreamJoin<>(this, otherImpl, prior, processor, topology);
    otherImpl.registerReceiver(stream.receiverForOtherStream);

    return chain(stream);
  }

}
