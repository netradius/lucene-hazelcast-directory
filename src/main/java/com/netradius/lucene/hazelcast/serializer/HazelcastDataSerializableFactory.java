package com.netradius.lucene.hazelcast.serializer;

import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.netradius.lucene.hazelcast.directory.HFile;

/**
 * Hazelcast data serializer.
 *
 * @author Dilip S Sisodia
 */
public class HazelcastDataSerializableFactory implements DataSerializableFactory {

  public static final int FACTORY_ID = 1;

  public static final int HFILE_TYPE = 1;

  public IdentifiedDataSerializable create(int typeId) {
    if (typeId == HFILE_TYPE) {
      return new HFile();
    } else {
      return null;
    }
  }
}
