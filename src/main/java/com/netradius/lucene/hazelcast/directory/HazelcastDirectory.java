package com.netradius.lucene.hazelcast.directory;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.netradius.lucene.hazelcast.serializer.HazelcastDataSerializableFactory;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.SingleInstanceLockFactory;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Hazelcast directory implementation.
 *
 * @author Dilip S Sisodia
 */
public class HazelcastDirectory extends BaseDirectory implements Accountable {

  protected HazelcastInstance hazelcastInstance;
  public static final int BUFFER_SIZE = 1024;
  protected final AtomicLong sizeInBytes = new AtomicLong();
  IMap<String, HFile> store;

  public HazelcastDirectory(HazelcastInstance hazelcastInstance, LockFactory lockFactory) {
    super(lockFactory);
    this.hazelcastInstance = hazelcastInstance;
    this.store = hazelcastInstance.getMap("hazelcastDirectory");
  }

  @Override
  public String[] listAll() throws IOException {
    String[] files = new String[store.size()];
    int index = 0;

    for (String file : store.keySet()) {
      files[index++] = file;
    }

    return files;
  }

  @Override
  public void deleteFile(final String name) throws IOException {
    ensureOpen();
    HFile file = store.remove(name);
    if (file != null) {
      sizeInBytes.addAndGet(-file.sizeInBytes);
    } else {
      throw new FileNotFoundException(name);
    }
  }

  @Override
  public long fileLength(final String name) throws IOException {
    ensureOpen();
    if (!store.containsKey(name)) {
      throw new FileNotFoundException(name);
    }
    return store.get(name).getLength();
  }


  @Override
  public void close() throws IOException {
    isOpen = false;
    store.clear();
  }

  @Override
  public IndexOutput createOutput(String s, IOContext ioContext) throws IOException {
    ensureOpen();
    HFile file = new HFile(this);
    store.put(s, file);
    return new HOutputStream(s, file, store);
  }

  @Override
  public void sync(Collection<String> names) throws IOException {
  }

  @Override
  public void renameFile(String source, String dest) throws IOException {
    ensureOpen();
    HFile file = this.store.get(source);
    if (file == null) {
      throw new FileNotFoundException(source);
    } else {
      this.store.put(dest, file);
      this.store.remove(source);
    }
  }

  @Override
  public IndexInput openInput(String s, IOContext ioContext) throws IOException {
    ensureOpen();
    if (!store.containsKey(s)) {
      throw new FileNotFoundException(s);
    }
    return new HInputStream(store.get(s), s);
  }

  @Override
  public long ramBytesUsed() {
    ensureOpen();
    return sizeInBytes.get();
  }

  @Override
  public Collection<Accountable> getChildResources() {
    return Accountables.namedAccountables("file", this.store);
  }
}
