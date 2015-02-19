package org.apache.samza.serializers.zk;

import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.samza.serializers.JsonSerde;

public class ZkJsonSerde extends JsonSerde implements ZkSerializer {
  @Override
  public byte[] serialize(Object data) throws ZkMarshallingError {
    return toBytes(data);
  }

  @Override
  public Object deserialize(byte[] bytes) throws ZkMarshallingError {
    return fromBytes(bytes);
  }
}
