package com.netradius.lucene.hazelcast.directory;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.netradius.lucene.hazelcast.serializer.HazelcastDataSerializableFactory;
import org.apache.lucene.util.Accountable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * Holds details for a file in HazelcastDirectory.
 *
 * @author Dilip S Sisodia
 */
public class HFile implements Accountable, IdentifiedDataSerializable {

  protected ArrayList<byte[]> buffers = new ArrayList<>();
  protected long sizeInBytes;
  long length;
  HazelcastDirectory directory;

  public HFile() {
  }

  public HFile(HazelcastDirectory directory) {
    this.directory = directory;
  }

  public synchronized long getLength() {
    return this.length;
  }

  public synchronized void setLength(long length) {
    this.length = length;
  }

  public synchronized long getSizeInBytes() {
    return sizeInBytes;
  }

  protected final byte[] addBuffer(int size) {
    byte[] buffer = newBuffer(size);
    synchronized (this) {
      buffers.add(buffer);
      sizeInBytes += size;
    }
    if (this.directory != null) {
      this.directory.sizeInBytes.getAndAdd(size);
    }
    return buffer;
  }

  protected final synchronized byte[] getBuffer(int index) {
    return buffers.get(index);
  }

  protected final synchronized int numBuffers() {
    return buffers.size();
  }

  protected byte[] newBuffer(int size) {
    return new byte[size];
  }

  public long ramBytesUsed() {
    return this.sizeInBytes;
  }

  public Collection<Accountable> getChildResources() {
    return Collections.emptyList();
  }

  public int getFactoryId() {
    return HazelcastDataSerializableFactory.FACTORY_ID;
  }

  public int getId() {
    return HazelcastDataSerializableFactory.HFILE_TYPE;
  }

  public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
    objectDataOutput.writeLong(length);
    objectDataOutput.writeLong(sizeInBytes);
    objectDataOutput.writeObject(buffers);
  }

  public void readData(ObjectDataInput objectDataInput) throws IOException {
    length = objectDataInput.readLong();
    sizeInBytes = objectDataInput.readLong();
    buffers = objectDataInput.readObject();
  }

}
