package dev.rmaiun.experiments.reactor;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class CustomStringDeser implements Deserializer<String> {

  @Override
  public String deserialize(String topic, byte[] data) {
    if (data.length == 4) {
      return String.valueOf(new IntegerDeserializer().deserialize(topic, data));
    } else {
      return new StringDeserializer().deserialize(topic, data);
    }
  }
}
