package com.netradius.lucene.hazelcast.directoryprovider;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.netradius.lucene.hazelcast.directory.HazelcastDirectory;
import com.netradius.lucene.hazelcast.serializer.HazelcastDataSerializableFactory;
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

  protected HazelcastInstance hazelcastInstance;
  protected String prefix;
  protected HazelcastDirectory directory;
  protected String indexName;
  protected Properties properties;
  protected ServiceManager serviceManager;

  public void initialize(String indexName,
                         Properties properties,
                         BuildContext buildContext) {

    this.indexName = indexName;
    this.properties = properties;
    this.serviceManager = buildContext.getServiceManager();

    this.prefix = properties.getProperty("hazelcast_prefix");
    String groupName = properties.getProperty("hazelcast_group_name");
    String groupPassword = properties.getProperty("hazelcast_group_password");
    String address = properties.getProperty("hazelcast_address");

    ClientConfig clientConfig = new ClientConfig();
    clientConfig.getNetworkConfig().getAddresses().add(address);
    clientConfig.getGroupConfig().setName(groupName);
    clientConfig.getGroupConfig().setPassword(groupPassword);

    clientConfig.getSerializationConfig().addDataSerializableFactory(
        HazelcastDataSerializableFactory.FACTORY_ID,
        new HazelcastDataSerializableFactory());

    this.hazelcastInstance = HazelcastClient.newHazelcastClient(clientConfig);
  }

  public void start(DirectoryBasedIndexManager directoryBasedIndexManager) {
    try {
      LockFactory lockFactory = serviceManager.requestService(LockFactoryCreator.class)
          .createLockFactory(null, properties);
      this.directory = new HazelcastDirectory(hazelcastInstance, prefix, indexName, lockFactory);
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
