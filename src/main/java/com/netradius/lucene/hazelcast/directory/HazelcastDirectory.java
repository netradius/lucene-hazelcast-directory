package com.netradius.lucene.hazelcast.directory;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
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
@Slf4j
public class HazelcastDirectory extends BaseDirectory implements Accountable {

  protected HazelcastInstance hazelcastInstance;
  protected final AtomicLong sizeInBytes = new AtomicLong();
  protected IMap<String, HFile> store;

  public HazelcastDirectory(HazelcastInstance hazelcastInstance,
      String indexName, LockFactory lockFactory) {
    super(lockFactory);
    this.hazelcastInstance = hazelcastInstance;
    this.store = hazelcastInstance.getMap(indexName);
  }

  @Override
  public String[] listAll() throws IOException {
    if (log.isTraceEnabled()) {
      log.trace("listAll()");
    }
    return store.keySet().toArray(new String[store.size()]);
  }

  @Override
  public void deleteFile(final String name) throws IOException {
    if (log.isTraceEnabled()) {
      log.trace("deleteFile(" + name + ")");
    }
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
    if (log.isTraceEnabled()) {
      log.trace("fileLength(" + name + ")");
    }
    ensureOpen();
    if (!store.containsKey(name)) {
      throw new FileNotFoundException(name);
    }
    HFile file = store.get(name);
    if (file != null) {
      return file.getLength();
    }
    throw new FileNotFoundException(name);
  }


  @Override
  public void close() throws IOException {
    if (log.isTraceEnabled()) {
      log.trace("close()");
    }
    isOpen = false;
  }

  @Override
  public IndexOutput createOutput(String s, IOContext ioContext) throws IOException {
    if (log.isTraceEnabled()) {
      log.trace("createOutout(" + s + "," + ioContext.toString() + ")");
    }
    ensureOpen();
    HFile file = new HFile(this);
    store.put(s, file);
    return new HOutputStream(s, file, store);
  }

  @Override
  public void sync(Collection<String> names) throws IOException {
    if (log.isTraceEnabled()) {
      log.trace("sync(" + names.toString() + ")");
    }
  }

  @Override
  public void renameFile(String source, String dest) throws IOException {
    if (log.isTraceEnabled()) {
      log.trace("renameFile(" + source + "," + dest + ")");
    }
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
    if (log.isTraceEnabled()) {
      log.trace("openInput(" + s + "," + ioContext.toString() + ")");
    }
    ensureOpen();
    if (!store.containsKey(s)) {
      throw new FileNotFoundException(s);
    }
    return new HInputStream(store.get(s), s);
  }

  @Override
  public long ramBytesUsed() {
    if (log.isTraceEnabled()) {
      log.trace("ramBytesUsed()");
    }
    ensureOpen();
    return sizeInBytes.get();
  }

  @Override
  public Collection<Accountable> getChildResources() {
    if (log.isTraceEnabled()) {
      log.trace("getChildResources()");
    }
    return Accountables.namedAccountables("file", this.store);
  }
}
