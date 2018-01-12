package com.netradius.lucene.hazelcast.directoryprovider;

import com.netradius.lucene.hazelcast.directory.HazelcastDirectory;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.store.LockFactory;
import org.hibernate.search.engine.service.spi.ServiceManager;
import org.hibernate.search.indexes.spi.DirectoryBasedIndexManager;
import org.hibernate.search.spi.BuildContext;
import org.hibernate.search.store.DirectoryProvider;
import org.hibernate.search.store.spi.DirectoryHelper;
import org.hibernate.search.store.spi.LockFactoryCreator;

import java.io.IOException;
import java.util.Properties;

/**
 * Hazelcast directory provider implementation.
 *
 * @author Dilip S Sisodia
 */
@Slf4j
public class HazelcastDirectoryProvider implements DirectoryProvider<HazelcastDirectory> {

  private HazelcastDirectory directory;
  private String indexName;
  private Properties properties;
  private ServiceManager serviceManager;

  public HazelcastDirectoryProvider() {
  }

  public void initialize(String directoryProviderName,
                         Properties properties,
                         BuildContext buildContext) {
    this.indexName = directoryProviderName;
    this.properties = properties;
    this.serviceManager = buildContext.getServiceManager();
  }

  public void start(DirectoryBasedIndexManager directoryBasedIndexManager) {
    try {
      LockFactory lockFactory = serviceManager.requestService(LockFactoryCreator.class)
          .createLockFactory(null, properties);
      this.directory = new HazelcastDirectory(lockFactory);
      this.properties = null;
      DirectoryHelper.initializeIndexIfNeeded(this.directory);
    } finally {
      serviceManager.releaseService(LockFactoryCreator.class);
    }

  }

  public void stop() {
    try {
      this.directory.close();
    } catch (IOException ex) {
      log.error("IOException: " + ex.getMessage());
    }
  }

  public HazelcastDirectory getDirectory() {
    return this.directory;
  }
}
