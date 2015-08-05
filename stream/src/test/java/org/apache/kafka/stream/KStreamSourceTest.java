package org.apache.kafka.stream;

import org.apache.kafka.stream.internal.PartitioningInfo;
import org.apache.kafka.stream.topology.KStreamTopology;
import org.apache.kafka.stream.topology.internal.KStreamMetadata;
import org.apache.kafka.stream.topology.internal.KStreamSource;
import org.apache.kafka.test.MockKStreamTopology;
import org.apache.kafka.test.MockProcessor;
import org.apache.kafka.test.MockKStreamContext;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class KStreamSourceTest {

  private String topicName = "topic";

  private KStreamMetadata streamMetadata = new KStreamMetadata(Collections.singletonMap(topicName, new PartitioningInfo(1)));

  @Test
  public void testKStreamSource() {

    KStreamTopology initializer = new MockKStreamTopology();
    MockProcessor<String, String> processor = new MockProcessor<>();

    KStreamSource<String, String> stream = new KStreamSource<>(null, initializer);
    stream.process(processor);

    final String[] expectedKeys = new String[] { "k1", "k2", "k3" };
    final String[] expectedValues = new String[] { "v1", "v2", "v3" };

    KStreamContext context = new MockKStreamContext(null, null);
    stream.bind(context, streamMetadata);
    for (int i = 0; i < expectedKeys.length; i++) {
      stream.receive(expectedKeys[i], expectedValues[i], 0L);
    }

    assertEquals(3, processor.processed.size());

    for (int i = 0; i < expectedKeys.length; i++) {
      assertEquals(expectedKeys[i] + ":" + expectedValues[i], processor.processed.get(i));
    }
  }

}
