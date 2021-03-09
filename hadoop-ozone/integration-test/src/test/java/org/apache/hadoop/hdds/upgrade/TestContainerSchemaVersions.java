/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.upgrade;

import org.apache.commons.codec.CharEncoding;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.RATIS;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT;
import static org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState.OPEN;

/**
 * Tests that DataNode will throw an exception on creation when it reads in a
 * VERSION file indicating a metadata layout version larger than its
 * software layout version.
 */
public class TestContainerSchemaVersions {
  @Rule
  public TemporaryFolder hddsMetadataDir = new TemporaryFolder();
  public TemporaryFolder omMetadataDir = new TemporaryFolder();
  public TemporaryFolder ozoneMetadataDir = new TemporaryFolder();

  private static final int NUM_DATA_NODES = 3;

  private OzoneConfiguration conf;
  private MiniOzoneCluster cluster;

  @Before
  public void setup() throws Exception {
    conf = new OzoneConfiguration();
    conf.setTimeDuration(HDDS_PIPELINE_REPORT_INTERVAL, 1000,
        TimeUnit.MILLISECONDS);
    conf.set(OZONE_DATANODE_PIPELINE_LIMIT, "1");
//    conf.set(HddsConfigKeys.HDDS_METADATA_DIR_NAME, hddsMetadataDir.toString());
//    conf.set(OMConfigKeys.OZONE_OM_DB_DIRS, omMetadataDir.toString());
    conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS, ozoneMetadataDir.toString());
    cluster = null;
  }

  @After
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testSchemaVersionField() throws Exception {
    // Pre finalized cluster (Current code using older metadata layout version).
    createNewCluster(HDDSLayoutFeature.INITIAL_VERSION);
    Set<Long> oldContainers = new HashSet<>();

    // When using old MLV, the new schema version field should not be
    // persisted to the container files.
    writeKey("old");
    assertReadKey("old");

    List<HddsDatanodeService> dnList = cluster.getHddsDatanodes();
    Assert.assertEquals(dnList.size(), NUM_DATA_NODES);

    for(HddsDatanodeService dn: dnList) {
      Iterator<Container<?>> contIter = dn.getDatanodeStateMachine()
          .getContainer().getContainerSet().getContainerIterator();

      // With 3 DNs and rep factor of 3, each DN should have at least one
      // container.
      Assert.assertTrue(contIter.hasNext());

      while(contIter.hasNext()) {
        Container<?> cont = contIter.next();
        assertSchemaVersion(cont, null);
        oldContainers.add(cont.getContainerData().getContainerID());
        // Simulate an upgrade, where all containers are closed first.
        cont.close();
      }
    }

    // When using new MLV, the new schema version field should be
    // persisted to container files for new container only.
    // Old container files should not be updated.
    createNewCluster(HDDSLayoutFeature.DATANODE_SCHEMA_V2);

    // When using old MLV, the new schema version field should not be
    // persisted to the container files.
    assertReadKey("old");
    writeKey("new");
    assertReadKey("new");

    dnList = cluster.getHddsDatanodes();
    Assert.assertEquals(dnList.size(), NUM_DATA_NODES);

    for(HddsDatanodeService dn: dnList) {
      Iterator<Container<?>> contIter = dn.getDatanodeStateMachine()
          .getContainer().getContainerSet().getContainerIterator();

      Assert.assertTrue(contIter.hasNext());
      boolean hasNewContainer = false;

      while(contIter.hasNext()) {
        Container<?> cont = contIter.next();
        if (oldContainers.contains(cont.getContainerData().getContainerID())) {
          assertSchemaVersion(cont, null);
        } else {
          assertSchemaVersion(cont, OzoneConsts.SCHEMA_V2);
          hasNewContainer = true;
        }
      }

      // Each DN should have created a container for the new key.
      Assert.assertTrue(hasNewContainer);
    }
  }

  // Pass null schema version to indicate it should not be in the file.
  private void assertSchemaVersion(Container<?> container,
     String schemaVersion) throws Exception {
    File contFile = container.getContainerFile();
    String fileContent = FileUtils.readFileToString(contFile,
        CharEncoding.UTF_8);

    final String schemaField =
        OzoneConsts.SCHEMA_VERSION + ": " + schemaVersion;

    if (schemaVersion != null) {
      Assert.assertTrue(fileContent.contains(schemaField));
    } else {
      Assert.assertFalse(fileContent.contains(OzoneConsts.SCHEMA_VERSION));
    }
  }

  /**
   * Creates a volume, bucket and key all with name {@code content}, whose
   * data is also {@code content}.
   */
  private void writeKey(String content) throws Exception {
    ObjectStore store = cluster.getClient().getObjectStore();
    store.createVolume(content);
    store.getVolume(content).createBucket(content);
    OzoneOutputStream stream =
        store.getVolume(content).getBucket(content)
        .createKey(content, 10, ReplicationType.RATIS,
            ReplicationFactor.THREE, new HashMap<>());

    stream.write(content.getBytes(UTF_8));
    stream.close();
  }

  /**
   * Reads a volume, bucket and key all with the name {@code content},
   * and checks that the key's data matches {@code content}.
   */
  private void assertReadKey(String content) throws Exception {
    ObjectStore store = cluster.getClient().getObjectStore();
    OzoneInputStream stream =
        store.getVolume(content).getBucket(content).readKey(content);

    byte[] bytes = new byte[content.length()];
    int numRead = stream.read(bytes);
    Assert.assertEquals(content.length(), numRead);
    Assert.assertEquals(content, new String(bytes, UTF_8));
  }

  private void createNewCluster(HDDSLayoutFeature layoutVersion)
      throws Exception {
    // Clean up old cluster if necessary.
    shutdown();
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(NUM_DATA_NODES)
        // allow only one FACTOR THREE pipeline.
        .setTotalPipelineNumLimit(NUM_DATA_NODES + 1)
        .setHbInterval(1000)
        .setHbProcessorInterval(1000)
        .setScmLayoutVersion(layoutVersion.layoutVersion())
        .setDnLayoutVersion(layoutVersion.layoutVersion())
        .build();
    cluster.waitForClusterToBeReady();

    // Wait for the one ratis factor 3 pipeline.
    PipelineManager scmPipelineManager = cluster.getStorageContainerManager().getPipelineManager();
    LambdaTestUtils.await(10000, 2000, () -> {
      List<Pipeline> pipelines =
          scmPipelineManager.getPipelines(RATIS, THREE, OPEN);
      return pipelines.size() == 1;
    });
  }
}