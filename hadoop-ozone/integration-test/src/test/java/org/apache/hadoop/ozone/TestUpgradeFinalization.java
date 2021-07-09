package org.apache.hadoop.ozone;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.OzoneConsts.LAYOUT_VERSION_KEY;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PrepareStatusResponse.PrepareStatus.PREPARE_COMPLETED;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.ALREADY_FINALIZED;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.FINALIZATION_DONE;
import static org.apache.ozone.test.GenericTestUtils.waitFor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.TestHelper;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.OzoneManagerPrepareState;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.StatusAndMessages;

import org.apache.ozone.test.LambdaTestUtils;
import org.apache.ratis.util.LifeCycle;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for OM upgrade finalization.
 */
@RunWith(Parameterized.class)
public class TestUpgradeFinalization {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestUpgradeFinalization.class);

  /**
   * Set a timeout for each test.
   */
  @Rule
  public Timeout timeout = new Timeout(300000);
  private MiniOzoneHAClusterImpl cluster;
  private int omFromLayoutVersion;
  private int scmFromLayoutVersion;
  private OzoneManagerProtocol omClient;
  private ContainerOperationClient scmClient;
  private ClientProtocol protocol;
  private ObjectStore objectStore;

  private static final String UPGRADE_CLIENT_ID = "finalize-test";

  /**
   * Defines a "from" layout version to finalize from.
   *
   * @return
   */
  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{
        {OMLayoutFeature.INITIAL_VERSION, HDDSLayoutFeature.SCM_HA},
    });
  }

  public TestUpgradeFinalization(OMLayoutFeature omFromVersion,
      HDDSLayoutFeature scmFromVersion) {
    this.omFromLayoutVersion = omFromVersion.layoutVersion();
    this.scmFromLayoutVersion = scmFromVersion.layoutVersion();
  }

  /**
   * Create a MiniDFSCluster for testing.
   */
  @Before
  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    String omServiceId = UUID.randomUUID().toString();
    cluster = (MiniOzoneHAClusterImpl) MiniOzoneCluster.newHABuilder(conf)
        .setClusterId(UUID.randomUUID().toString())
        .setScmId(UUID.randomUUID().toString())
        .setSCMServiceId(UUID.randomUUID().toString())
        .setOMServiceId(omServiceId)
        .setNumOfOzoneManagers(3)
        .setNumOfStorageContainerManagers(3)
        .setNumOfActiveSCMs(3)
        .setNumDatanodes(1)
        .setOmLayoutVersion(omFromLayoutVersion)
        .setScmLayoutVersion(scmFromLayoutVersion)
        .build();

    cluster.waitForClusterToBeReady();
    scmClient = new ContainerOperationClient(conf);
    objectStore = OzoneClientFactory.getRpcClient(omServiceId,
        conf).getObjectStore();
    protocol = objectStore.getClientProxy();
    omClient = protocol.getOzoneManagerClient();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @After
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Currently this is a No-Op finalization since there is only one layout
   * version in OM, and SCM HA is the latest HDDS layout version. But this test
   * is expected to remain consistent when a new version is added.
   */
  @Test
  public void testFinalization() throws Exception {
    // TODO: Clean up key reads/writes.
    protocol.createVolume("vol1");
    protocol.createBucket("vol1", "bucket1");
    writeTestData("vol1", "bucket1", "key1");

    finalizeOM();
    finalizeScm();

    OzoneInputStream stream = protocol.getKey("vol1", "bucket1","key1");
    assertEquals(100, stream.available());

    writeTestData("vol1", "bucket1", "key2");
    stream = protocol.getKey("vol1", "bucket1","key2");
    assertEquals(100, stream.available());
  }

  private void writeTestData(String volumeName,
      String bucketName, String keyName) throws Exception {

    String keyString = UUID.randomUUID().toString();
    byte[] data = ContainerTestHelper.getFixedLengthString(
        keyString, 100).getBytes(UTF_8);
    OzoneOutputStream keyStream = TestHelper.createKey(
        keyName, ReplicationType.STAND_ALONE, ReplicationFactor.ONE,
        100, objectStore, volumeName, bucketName);
    keyStream.write(data);
    keyStream.close();
  }

  private void finalizeOM() throws Exception {
    // Assert OM Layout Version is 'fromLayoutVersion' on deploy.
    for (OzoneManager om: cluster.getOzoneManagersList()) {
      assertTrue(checkOMLayoutVersions(om, omFromLayoutVersion, null));
    }

    StatusAndMessages response =
        omClient.finalizeUpgrade(UPGRADE_CLIENT_ID);
    logStatusAndMessages(response);

    if (!response.status().equals(ALREADY_FINALIZED)) {
      waitForOMFinalization();

      for (OzoneManager om: cluster.getOzoneManagersList()) {
        int lv = OMLayoutVersionManager.maxLayoutVersion();
        LambdaTestUtils.await(30000, 3000,
            () -> checkOMLayoutVersions(om, lv, Integer.toString(lv)));
      }
    } else {
      LOG.warn("OMs already finalized. Full finalization not tested.");
      for (OzoneManager om: cluster.getOzoneManagersList()) {
        assertTrue(checkOMLayoutVersions(om, omFromLayoutVersion, null));
      }
    }
  }

  private void finalizeScm() throws Exception {
    Set<String> pipelineIDs = getPipelineIDs();

    // Assert SCM Layout Version is 'fromLayoutVersion' on deploy.
    for (StorageContainerManager scm: cluster.getStorageContainerManagers()) {
      assertTrue(checkScmLayoutVersions(scm, scmFromLayoutVersion, null));
    }

    StatusAndMessages response =
        scmClient.finalizeScmUpgrade(UPGRADE_CLIENT_ID);
    logStatusAndMessages(response);

    if (!response.status().equals(ALREADY_FINALIZED)) {
      waitForScmFinalization();

      for (StorageContainerManager scm: cluster.getStorageContainerManagers()) {
        int lv = HDDSLayoutVersionManager.maxLayoutVersion();
        LambdaTestUtils.await(30000, 3000,
            () -> checkScmLayoutVersions(scm, lv, Integer.toString(lv)));
      }

      Set<String> newPipelineIDs = getPipelineIDs();
      for (String id: pipelineIDs) {
        assertFalse(newPipelineIDs.contains(id));
      }
    } else {
      LOG.warn("SCMs already finalized. Full finalization not tested.");
      for (StorageContainerManager scm: cluster.getStorageContainerManagers()) {
        assertTrue(checkScmLayoutVersions(scm, scmFromLayoutVersion, null));
      }
    }
  }

  @Test
  public void testFinalizationWithOneNodeDown() throws Exception {
    finalizeOMWithOneDown();
    finalizeScmWithOneDown();
  }

  private void finalizeOMWithOneDown() throws Exception {
    List<OzoneManager> runningOms = cluster.getOzoneManagersList();
    final int shutdownOMIndex = 2;
    OzoneManager downedOM = cluster.getOzoneManager(shutdownOMIndex);
    cluster.stopOzoneManager(shutdownOMIndex);
    Assert.assertFalse(downedOM.isRunning());
    Assert.assertEquals(runningOms.remove(shutdownOMIndex), downedOM);

    // Have to do a "prepare" operation to get rid of the logs in the active
    // OMs.
    long prepareIndex = omClient.prepareOzoneManager(120L, 5L);
    assertClusterPrepared(prepareIndex, runningOms);

    StatusAndMessages response =
        omClient.finalizeUpgrade(UPGRADE_CLIENT_ID);
    logStatusAndMessages(response);

    if (!response.status().equals(ALREADY_FINALIZED)) {
      waitForOMFinalization();
      cluster.restartOzoneManager(downedOM, true);

      try {
        waitFor(() -> downedOM.getOmRatisServer()
                .getOmStateMachine().getLifeCycleState().isPausingOrPaused(),
            1000, 60000);
      } catch (TimeoutException timeEx) {
        LifeCycle.State state = downedOM.getOmRatisServer()
            .getOmStateMachine().getLifeCycle().getCurrentState();
        if (state != LifeCycle.State.RUNNING) {
          Assert.fail("OM State Machine State expected to be in RUNNING state.");
        }
      }

      waitFor(() -> {
        LifeCycle.State lifeCycleState = downedOM.getOmRatisServer()
            .getOmStateMachine().getLifeCycle().getCurrentState();
        return !lifeCycleState.isPausingOrPaused();
      }, 1000, 60000);


      for (OzoneManager om : cluster.getOzoneManagersList()) {
        int lv = OMLayoutVersionManager.maxLayoutVersion();
        LambdaTestUtils.await(30000, 3000,
            () -> checkOMLayoutVersions(om, lv, Integer.toString(lv)));
      }
    } else {
      LOG.warn("OMs already finalized. Full finalization not tested.");
      for (OzoneManager om: cluster.getOzoneManagersList()) {
        assertTrue(checkOMLayoutVersions(om, omFromLayoutVersion, null));
      }
    }
  }

  private void finalizeScmWithOneDown() throws Exception {
    Set<String> pipelineIDs = getPipelineIDs();

    // TODO: Test with snapshot (without prepare, currently only testing
    //  normal apply).
    List<StorageContainerManager> runningScms =
        cluster.getStorageContainerManagersList();
    final int shutdownScmIndex = 2;
    StorageContainerManager downedScm =
        cluster.getStorageContainerManager(shutdownScmIndex);
    cluster.shutdownStorageContainerManager(downedScm);
//    Assert.assertFalse(downedScm.isRunning());
    Assert.assertEquals(runningScms.remove(shutdownScmIndex), downedScm);

    StatusAndMessages response =
        scmClient.finalizeScmUpgrade(UPGRADE_CLIENT_ID);
    logStatusAndMessages(response);

    if (response.status().equals(ALREADY_FINALIZED)) {
      waitForScmFinalization();
      cluster.restartStorageContainerManager(downedScm, true);

      try {
        waitFor(() -> downedScm.getScmHAManager().getRatisServer()
                .getSCMStateMachine().getLifeCycleState().isPausingOrPaused(),
            1000, 60000);
      } catch (TimeoutException timeEx) {
        LifeCycle.State state = downedScm.getScmHAManager().getRatisServer()
            .getSCMStateMachine().getLifeCycle().getCurrentState();
        if (state != LifeCycle.State.RUNNING) {
          Assert.fail("SCM State Machine State expected to be in RUNNING state.");
        }
      }

      waitFor(() -> {
        LifeCycle.State lifeCycleState = downedScm.getScmHAManager()
            .getRatisServer().getSCMStateMachine().getLifeCycle()
            .getCurrentState();
        return !lifeCycleState.isPausingOrPaused();
      }, 1000, 60000);

      for (StorageContainerManager scm :
          cluster.getStorageContainerManagersList()) {
        int lv = HDDSLayoutVersionManager.maxLayoutVersion();
        LambdaTestUtils.await(30000, 3000,
            () -> checkScmLayoutVersions(scm, lv, Integer.toString(lv)));
      }

      Set<String> newPipelineIDs = getPipelineIDs();
      for (String id: pipelineIDs) {
        assertFalse(newPipelineIDs.contains(id));
      }
    } else {
      LOG.warn("SCMs already finalized. Full finalization not tested.");
      for (StorageContainerManager scm: cluster.getStorageContainerManagers()) {
        assertTrue(checkScmLayoutVersions(scm, scmFromLayoutVersion, null));
      }
    }
  }

  private void assertClusterPrepared(
      long preparedIndex, List<OzoneManager> ozoneManagers) throws Exception {
    for (OzoneManager om : ozoneManagers) {
      LambdaTestUtils.await(120000,
          1000, () -> {
            if (!om.isRunning()) {
              return false;
            } else {
              boolean preparedAtIndex = false;
              OzoneManagerPrepareState.State state =
                  om.getPrepareState().getState();

              if (state.getStatus() == PREPARE_COMPLETED) {
                if (state.getIndex() == preparedIndex) {
                  preparedAtIndex = true;
                } else {
                  // State will not change if we are prepared at the wrong
                  // index. Break out of wait.
                  throw new Exception("OM " + om.getOMNodeId() + " prepared " +
                      "but prepare index " + state.getIndex() + " does not " +
                      "match expected prepare index " + preparedIndex);
                }
              }
              return preparedAtIndex;
            }
          });
    }
  }

  private void waitForOMFinalization()
      throws Exception {
    waitForFinalization(() -> {
      try {
        return omClient.queryUpgradeFinalizationProgress(UPGRADE_CLIENT_ID,
            false, false);
      } catch (IOException e) {
        Assert.fail(e.getMessage());
        return null;
      }
    });
  }

  private void waitForScmFinalization()
      throws Exception {
    waitForFinalization(() -> {
      try {
        return scmClient.queryUpgradeFinalizationProgress(UPGRADE_CLIENT_ID,
            false, false);
      } catch (IOException e) {
        Assert.fail(e.getMessage());
        return null;
      }
    });
  }

  private void waitForFinalization(Supplier<StatusAndMessages> finalizationCheck)
      throws TimeoutException, InterruptedException {
    waitFor(() -> {
      StatusAndMessages statusAndMessages = finalizationCheck.get();
      logStatusAndMessages(statusAndMessages);
      return statusAndMessages.status().equals(FINALIZATION_DONE);
    }, 2000, 20000);
  }

  private void logStatusAndMessages(StatusAndMessages response) {
    LOG.info("Finalization status: {} messages: {}", response.status(),
        response.msgs());
  }

  private boolean checkOMLayoutVersions(OzoneManager om, int mlv,
      String dbValue) throws IOException {
    int lv = om.getVersionManager().getMetadataLayoutVersion();
    String dbLv =
        om.getMetadataManager().getMetaTable().get(LAYOUT_VERSION_KEY);
    LOG.info("OM {} MLV: {} DB layout version: {}", om.getOMNodeId(), lv, dbLv);
    return (mlv == lv) && (dbValue.equals(dbLv));
  }

  private boolean checkScmLayoutVersions(StorageContainerManager scm, int mlv,
      String dbValue) throws IOException {
    int lv = scm.getLayoutVersionManager().getMetadataLayoutVersion();
    String dbLv =
        scm.getScmMetadataStore().getMetaTable().get(LAYOUT_VERSION_KEY);
    LOG.info("SCM {} MLV: {} DB layout version: {}", scm.getSCMNodeId(), lv,
        dbLv);
    return (mlv == lv) && (dbValue.equals(dbLv));
  }

  private Set<String> getPipelineIDs() {
    ReplicationConfig config =
        ReplicationConfig.fromTypeAndFactor(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.ONE);
    List<Pipeline> pipelines =
    cluster.getStorageContainerManager().getPipelineManager()
        .getPipelines(config, Pipeline.PipelineState.OPEN);

    Set<String> pipelineIDs =
        pipelines.stream().map(p -> p.getId().toString()).collect(Collectors.toSet());
    Assert.assertTrue(pipelineIDs.size() >= 1);
    return pipelineIDs;
  }
}
