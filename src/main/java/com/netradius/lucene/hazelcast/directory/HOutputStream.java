package com.netradius.lucene.hazelcast.directory;

import com.hazelcast.core.IMap;
import org.apache.lucene.store.BufferedChecksum;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Accountable;

import java.io.IOException;
import java.util.Collection;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/**
 * Random access Output stream for output to a file in directory, used for lucene index output
 * operations.
 *
 * @author Dilip S Sisodia
 */
public class HOutputStream extends IndexOutput implements Accountable {
  static final int BUFFER_SIZE = 1024;
  private final Checksum crc;
  boolean dirty;
  byte[] currentBuffer;
  int bufferLength;
  int bufferPosition;
  int currentBufferIndex;
  long bufferStart;
  IMap<String, HFile> store;
  HFile file;
  String name;

  public HOutputStream() {
    this("unnamed", new HFile(), true);
  }

  public HOutputStream(String name, HFile file) {
    this(name, file, true);
  }

  public HOutputStream(HFile file, boolean checksum) {
    this("unnamed", file, checksum);
  }

  public HOutputStream(String name, HFile file, boolean checksum) {
    super("HOutputStream(name=\"" + name + "\")");
    this.file = file;
    this.currentBufferIndex = -1;
    this.currentBuffer = null;
    if (checksum) {
      this.crc = new BufferedChecksum(new CRC32());
    } else {
      this.crc = null;
    }
  }

  public HOutputStream(final String name, final HFile f,
                       final IMap<String, HFile> store) {
    super("HOutputStream(name=\"" + name + "\")");
    file = f;
    this.name = name;
    this.store = store;
    currentBufferIndex = -1;
    currentBuffer = null;
    crc = new BufferedChecksum(new CRC32());
  }

  public void reset() {
    currentBuffer = null;
    currentBufferIndex = -1;
    bufferPosition = 0;
    bufferStart = 0;
    bufferLength = 0;
    file.setLength(0);

    crc.reset();

  }

  @Override
  public void close() throws IOException {
    if (dirty) {
      flush();
    }
  }

  @Override
  public void writeByte(final byte b) throws IOException {
    if (bufferPosition == bufferLength) {
      currentBufferIndex++;
      switchCurrentBuffer();
    }

    crc.update(b);

    currentBuffer[bufferPosition++] = b;
    dirty = true;
  }

  @Override
  public void writeBytes(final byte[] b, int offset, int len) throws IOException {
    assert b != null;

    crc.update(b, offset, len);

    while (len > 0) {
      if (bufferPosition == bufferLength) {
        currentBufferIndex++;
        switchCurrentBuffer();
      }

      int remainInBuffer = currentBuffer.length - bufferPosition;
      int bytesToCopy = len < remainInBuffer ? len : remainInBuffer;
      System.arraycopy(b, offset, currentBuffer, bufferPosition, bytesToCopy);
      offset += bytesToCopy;
      len -= bytesToCopy;
      bufferPosition += bytesToCopy;
    }

    dirty = true;
  }

  private void switchCurrentBuffer() throws IOException {
    if (currentBufferIndex == file.numBuffers()) {
      currentBuffer = file.addBuffer(BUFFER_SIZE);
    } else {
      currentBuffer = file.getBuffer(currentBufferIndex);
    }
    bufferPosition = 0;
    bufferStart = (long) BUFFER_SIZE * (long) currentBufferIndex;
    bufferLength = currentBuffer.length;
  }

  private void setFileLength() {
    long pointer = bufferStart + bufferPosition;
    if (pointer > file.length) {
      file.setLength(pointer);
    }
  }

  public void flush() throws IOException {
    setFileLength();
    store.put(name, file);
    dirty = false;
  }

  @Override
  public long getFilePointer() {
    return currentBufferIndex < 0 ? 0 : bufferStart + bufferPosition;
  }

  @Override
  public long ramBytesUsed() {
    return (long) file.numBuffers() * (long) BUFFER_SIZE;
  }

  @Override
  public Collection<Accountable> getChildResources() {
    return null;
  }

  @Override
  public long getChecksum() throws IOException {
    if (crc == null) {
      throw new IllegalStateException("internal MapDirectoryOutputStream created with "
          + "checksum disabled");
    } else {
      return crc.getValue();
    }
  }
}
