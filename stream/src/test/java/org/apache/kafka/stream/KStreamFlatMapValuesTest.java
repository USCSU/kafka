package org.apache.kafka.stream;

import org.apache.kafka.stream.internal.PartitioningInfo;
import org.apache.kafka.stream.topology.KStreamTopology;
import org.apache.kafka.stream.topology.ValueMapper;
import org.apache.kafka.stream.topology.internal.KStreamMetadata;
import org.apache.kafka.stream.topology.internal.KStreamSource;
import org.apache.kafka.test.MockKStreamTopology;
import org.apache.kafka.test.MockProcessor;
import org.apache.kafka.test.MockKStreamContext;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class KStreamFlatMapValuesTest {

  private String topicName = "topic";

  private KStreamMetadata streamMetadata = new KStreamMetadata(Collections.singletonMap(topicName, new PartitioningInfo(1)));

  @Test
  public void testFlatMapValues() {

    ValueMapper<Iterable<String>, String> mapper =
      new ValueMapper<Iterable<String>, String>() {
      @Override
      public Iterable<String> apply(String value) {
        ArrayList<String> result = new ArrayList<String>();
        result.add(value.toLowerCase());
        result.add(value);
        return result;
      }
    };

    final int[] expectedKeys = new int[] { 0, 1, 2, 3 };

    KStreamTopology initializer = new MockKStreamTopology();
    KStreamSource<Integer, String> stream;
    MockProcessor<Integer, String> processor;

    processor = new MockProcessor<>();
    stream = new KStreamSource<>(null, initializer);
    stream.flatMapValues(mapper).process(processor);

    KStreamContext context = new MockKStreamContext(null, null);
    stream.bind(context, streamMetadata);
    for (int i = 0; i < expectedKeys.length; i++) {
      stream.receive(expectedKeys[i], "V" + expectedKeys[i], 0L);
    }

    assertEquals(8, processor.processed.size());

    String[] expected = new String[] { "0:v0", "0:V0", "1:v1", "1:V1", "2:v2", "2:V2", "3:v3", "3:V3" };

    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], processor.processed.get(i));
    }
  }

}
