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

package org.apache.hadoop.ozone.genesis;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager;
import org.apache.hadoop.hdds.scm.server.SCMConfigurator;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.commons.lang3.RandomStringUtils;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Benchmarks OzoneManager.
 */
@State(Scope.Thread)
public class BenchMarkOzoneManager {

  private static String testDir;
  private static OzoneManager om;
  private static StorageContainerManager scm;
  private static ReentrantLock lock = new ReentrantLock();
  private static String volumeName = UUID.randomUUID().toString();
  private static String bucketName = UUID.randomUUID().toString();
  private static List<String> keyNames = new ArrayList<>();
  private static List<Long> clientIDs = new ArrayList<>();

  private static int numPipelines = 1;
  private static int numContainersPerPipeline = 3;

  @Setup(Level.Trial)
  public static void initialize()
      throws Exception {
    try {
      lock.lock();
      if (scm == null) {
        OzoneConfiguration conf = new OzoneConfiguration();
        testDir = GenesisUtil.getTempPath()
            .resolve(RandomStringUtils.randomNumeric(7)).toString();
        conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir);

        GenesisUtil.configureSCM(conf, 10);
        GenesisUtil.configureOM(conf, 20);
        conf.setInt(OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT,
            numContainersPerPipeline);
        GenesisUtil.addPipelines(ReplicationFactor.THREE, numPipelines, conf);

        scm = GenesisUtil.getScm(conf, new SCMConfigurator());
        scm.start();
        om = GenesisUtil.getOm(conf);
        om.start();

        // prepare SCM
        PipelineManager pipelineManager = scm.getPipelineManager();
        for (Pipeline pipeline : pipelineManager
            .getPipelines(
                new RatisReplicationConfig(ReplicationFactor.THREE))) {
          pipelineManager.openPipeline(pipeline.getId());
        }
        scm.getEventQueue().fireEvent(SCMEvents.SAFE_MODE_STATUS,
            new SCMSafeModeManager.SafeModeStatus(false, false));
        Thread.sleep(1000);

        // prepare OM
        om.createVolume(new OmVolumeArgs.Builder().setVolume(volumeName)
            .setAdminName(UserGroupInformation.getLoginUser().getUserName())
            .setOwnerName(UserGroupInformation.getLoginUser().getUserName())
            .build());
        om.createBucket(new OmBucketInfo.Builder().setBucketName(bucketName)
            .setVolumeName(volumeName).build());
        createKeys(100000);
      }
    } finally {
      lock.unlock();
    }
  }

  private static void createKeys(int numKeys) throws IOException {
    for (int i = 0; i < numKeys; i++) {
      String key = UUID.randomUUID().toString();
      OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
          .setVolumeName(volumeName)
          .setBucketName(bucketName)
          .setKeyName(key)
          .setDataSize(0)
          .setReplicationConfig(
              new RatisReplicationConfig(ReplicationFactor.THREE))
          .build();
      OpenKeySession keySession = om.getKeyManager().openKey(omKeyArgs);
      long clientID = keySession.getId();
      keyNames.add(key);
      clientIDs.add(clientID);
    }
  }

  @TearDown(Level.Trial)
  public static void tearDown() {
    try {
      lock.lock();
      if (scm != null) {
        scm.stop();
        scm.join();
        scm = null;
        om.stop();
        om.join();
        om = null;
        FileUtil.fullyDelete(new File(testDir));
      }
    } finally {
      lock.unlock();
    }
  }

  @Threads(4)
  @Benchmark
  public void allocateBlockBenchMark(BenchMarkOzoneManager state,
      Blackhole bh) throws IOException {
    int index = (int) (Math.random() * keyNames.size());
    String key = keyNames.get(index);
    OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(key)
        .setDataSize(50)
        .setReplicationConfig(
            new RatisReplicationConfig(ReplicationFactor.THREE))
        .build();
    state.om.allocateBlock(omKeyArgs, clientIDs.get(index), new ExcludeList());
  }

  @Threads(4)
  @Benchmark
  public void createAndCommitKeyBenchMark(BenchMarkOzoneManager state,
      Blackhole bh) throws IOException {
    String key = UUID.randomUUID().toString();
    OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(key)
        .setDataSize(50)
        .setReplicationConfig(
            new RatisReplicationConfig(ReplicationFactor.THREE))
        .build();
    OpenKeySession openKeySession = state.om.openKey(omKeyArgs);
    state.om.allocateBlock(omKeyArgs, openKeySession.getId(),
        new ExcludeList());
  }
}
