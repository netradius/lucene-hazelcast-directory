package com.netradius.lucene.hazelcast.directory;

import org.apache.lucene.store.IndexInput;

import java.io.EOFException;
import java.io.IOException;

/**
 * Random access input stream for input from a file in directory, used for lucene index input
 * operations.
 *
 * @author Dilip S Sisodia
 */
public class HInputStream extends IndexInput implements Cloneable {

  static final int BUFFER_SIZE = HOutputStream.BUFFER_SIZE;

  private final HFile file;
  private final long length;


  private byte[] currentBuffer;
  private int currentBufferIndex;

  private int bufferPosition;
  private long bufferStart;
  private int bufferLength;

  public HInputStream(HFile file, String name) throws IOException {
    this(file, name, file.length);
  }

  public HInputStream(HFile file, String name, long length) throws IOException {
    super("HInputStream(name=" + name + ")");
    this.file = file;
    this.length = length;
    if (length / BUFFER_SIZE >= Integer.MAX_VALUE) {
      throw new IOException("RAMInputStream too large length=" + length + ": " + name);
    }
    currentBufferIndex = -1;
    currentBuffer = null;
  }

  @Override
  public void close() {
  }

  @Override
  public long length() {
    return this.length;
  }

  public byte readByte() throws IOException {
    if (this.bufferPosition >= this.bufferLength) {
      ++this.currentBufferIndex;
      this.switchCurrentBuffer(true);
    }

    return this.currentBuffer[this.bufferPosition++];
  }

  public void readBytes(byte[] b, int offset, int len) throws IOException {
    while (len > 0) {
      if (this.bufferPosition >= this.bufferLength) {
        ++this.currentBufferIndex;
        this.switchCurrentBuffer(true);
      }

      int remainInBuffer = this.bufferLength - this.bufferPosition;
      int bytesToCopy = len < remainInBuffer ? len : remainInBuffer;
      System.arraycopy(this.currentBuffer, this.bufferPosition, b, offset, bytesToCopy);
      offset += bytesToCopy;
      len -= bytesToCopy;
      this.bufferPosition += bytesToCopy;
    }

  }

  private final void switchCurrentBuffer(boolean enforceEOF) throws IOException {
    this.bufferStart = BUFFER_SIZE * (long) this.currentBufferIndex;
    if (this.bufferStart <= this.length && this.currentBufferIndex < this.file.numBuffers()) {
      this.currentBuffer = this.file.getBuffer(this.currentBufferIndex);
      this.bufferPosition = 0;
      long buflen = this.length - this.bufferStart;
      this.bufferLength = buflen > BUFFER_SIZE ? BUFFER_SIZE : (int) buflen;
    } else {
      if (enforceEOF) {
        throw new EOFException("read past EOF: " + this);
      }

      --this.currentBufferIndex;
      this.bufferPosition = BUFFER_SIZE;
    }

  }

  public long getFilePointer() {
    return this.currentBufferIndex < 0 ? 0L : this.bufferStart + (long) this.bufferPosition;
  }

  public void seek(long pos) throws IOException {
    if (this.currentBuffer == null || pos < this.bufferStart
        || pos >= this.bufferStart + BUFFER_SIZE) {
      this.currentBufferIndex = (int) (pos / BUFFER_SIZE);
      this.switchCurrentBuffer(false);
    }

    this.bufferPosition = (int) (pos % BUFFER_SIZE);
  }

  public IndexInput slice(String sliceDescription, final long offset, long length)
      throws IOException {
    if (offset >= 0L && length >= 0L && offset + length <= this.length) {
      return new HInputStream(
          this.file, this.getFullSliceDescription(sliceDescription), offset + length) {
        {
          this.seek(0L);
        }

        public void seek(long pos) throws IOException {
          if (pos < 0L) {
            throw new IllegalArgumentException("Seeking to negative position: " + this);
          } else {
            super.seek(pos + offset);
          }
        }

        public long getFilePointer() {
          return super.getFilePointer() - offset;
        }

        public long length() {
          return super.length() - offset;
        }

        public IndexInput slice(String sliceDescription, long ofs, long len) throws IOException {
          return super.slice(sliceDescription, offset + ofs, len);
        }
      };
    } else {
      throw new IllegalArgumentException("slice() " + sliceDescription + " out of bounds: " + this);
    }
  }
}
