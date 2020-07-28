/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.common;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.utils.ReferenceCountedDB;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil;
//import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
//import org.apache.hadoop.ozone.container.testutils.BlockDeletingServiceTestImpl;
//import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static org.junit.Assert.*;

/**
 * Tests reading of containers written with DB schema version 1, which will
 * have different column family layout than newer schema versions.
 * These tests only need to read data from these containers, since any new
 * containers will be created using the latest schema version.
 */
public class TestOldSchemaVersions {
  private OzoneConfiguration conf;

  private File metadataDir;
  private File dbFile;

  // TODO : Rename this and access fields statically.
  private TestDB db;

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Before
  public void setup() throws Exception {
    conf = new OzoneConfiguration();
    db = new TestDB();

    // Copy data to the temporary folder so it can be safely modified.
    File tempMetadataDir =
            tempFolder.newFolder(Long.toString(db.CONTAINER_ID),
                    OzoneConsts.CONTAINER_META_PATH);

    FileUtils.copyDirectoryToDirectory(db.getDBDirectory(), tempMetadataDir);
    FileUtils.copyFileToDirectory(db.getContainerFile(), tempMetadataDir);

    metadataDir = tempMetadataDir;
    dbFile = metadataDir.listFiles((dir, name) -> name.equals(db.DB_NAME))[0];
  }

  /**
   * Counts the number of #deleted#, #deleting#, and unprefixed blocks in the
   * database, and checks that they match the expected values.
   * @throws IOException
   */
  @Test
  public void testBlockIteration() throws IOException {
    KeyValueContainerData kvData = newKvData();
    KeyValueContainerUtil.parseKVContainerData(kvData, conf);

    try(ReferenceCountedDB refCountedDB = BlockUtils.getDB(kvData, conf)) {
      assertEquals(db.NUM_DELETED_BLOCKS, countDeletedBlocks(refCountedDB));

      assertEquals(db.NUM_PENDING_DELETION_BLOCKS,
              countDeletingBlocks(refCountedDB));

      assertEquals(db.KEY_COUNT,
              countUnprefixedBlocks(refCountedDB));
    }
  }

  /**
   * Tests reading of a container that was written in schema version 1, when
   * the container has metadata keys present.
   * The {@link KeyValueContainerUtil} will read these values to fill in a
   * {@link KeyValueContainerData} object.
   * @throws Exception
   */
  @Test
  public void testReadWithMetadata() throws Exception {
    KeyValueContainerData kvData = newKvData();
    KeyValueContainerUtil.parseKVContainerData(kvData, conf);
    checkContainerData(kvData);
  }

  /**
   * Tests reading of a container that was written in schema version 1, when
   * the container has no metadata keys present.
   * The {@link KeyValueContainerUtil} will scan the blocks in the database
   * to fill these metadata values into the database and into a
   * {@link KeyValueContainerData} object.
   * @throws Exception
   */
  @Test
  public void testReadWithoutMetadata() throws Exception {
    // Init the kvData enough values so we can get the database to modify for
    // testing and then read.
    KeyValueContainerData kvData = newKvData();
    KeyValueContainerUtil.parseKVContainerData(kvData, conf);

    // Delete metadata keys from our copy of the DB.
    // This simulates them not being there to start with.
    try (ReferenceCountedDB db = BlockUtils.getDB(kvData, conf)) {
      Table<String, Long> metadataTable = db.getStore().getMetadataTable();

      metadataTable.delete(OzoneConsts.BLOCK_COUNT);
      assertNull(metadataTable.get(OzoneConsts.BLOCK_COUNT));

      metadataTable.delete(OzoneConsts.CONTAINER_BYTES_USED);
      assertNull(metadataTable.get(OzoneConsts.CONTAINER_BYTES_USED));

      metadataTable.delete(OzoneConsts.PENDING_DELETE_BLOCK_COUNT);
      assertNull(metadataTable.get(OzoneConsts.PENDING_DELETE_BLOCK_COUNT));
    }

    // Create a new container data object, and fill in its metadata by
    // counting blocks from the database, since the metadata keys in the
    // database are now gone.
    kvData = newKvData();
    KeyValueContainerUtil.parseKVContainerData(kvData, conf);
    checkContainerData(kvData);
  }

  /**
   * Tests reading blocks marked for deletion from a container written in
   * schema version 1. Because the block deleting service both reads for
   * deleted blocks and deletes them, this test will modify its copy of the
   * database.
   */
  @Test
  public void testDelete() throws Exception {
//    final int numBlocksToDelete = 2;
//
//    // TODO : Figure out how to construct this.
//    OzoneContainer container = new OzoneContainer();
//    BlockDeletingServiceTestImpl service =
//            new BlockDeletingServiceTestImpl(container,
//            1000, conf);
//    service.start();
//    GenericTestUtils.waitFor(service::isStarted, 100, 3000);
//    service.runDeletingTasks();
//    GenericTestUtils.waitFor(()
//        -> service.getTimesOfProcessed() == 1,
//        00, 3000);
//
//    try(ReferenceCountedDB refCountedDB = BlockUtils.getDB(newKvData(), conf)) {
//      // Blocks marked with #deleting# prefix should be deleted.
//      assertEquals(db.NUM_PENDING_DELETION_BLOCKS - numBlocksToDelete,
//              countDeletingBlocks(refCountedDB));
//
//      // All other blocks should remain unchanged.
//      assertEquals(db.NUM_DELETED_BLOCKS, countDeletedBlocks(refCountedDB));
//      assertEquals(db.KEY_COUNT, countUnprefixedBlocks(refCountedDB));
//
//      // Since metadata is being stored in the same table, make sure it is not
//      // altered as well.
//      Table<String, Long> metadataTable =
//              refCountedDB.getStore().getMetadataTable();
//      assertEquals(db.KEY_COUNT,
//              (long)metadataTable.get(OzoneConsts.BLOCK_COUNT));
//      assertEquals(db.BYTES_USED,
//              (long)metadataTable.get(OzoneConsts.CONTAINER_BYTES_USED));
//    }
  }

  /**
   * @return A {@link KeyValueContainerData} object that has only its
   * metadata path, db file, and checksum set to match the database under test.
   * @throws IOException
   */
  private KeyValueContainerData newKvData() throws IOException {
    ChunkLayOutVersion clVersion =
            ChunkLayOutVersion.getChunkLayOutVersion(1);
    String pipelineID = UUID.randomUUID().toString();
    String nodeID = UUID.randomUUID().toString();

    KeyValueContainerData kvData = new KeyValueContainerData(db.CONTAINER_ID,
            clVersion, ContainerTestHelper.CONTAINER_MAX_SIZE,
            pipelineID, nodeID);
    kvData.setMetadataPath(metadataDir.getAbsolutePath());
    kvData.setDbFile(dbFile);

    Yaml yaml = ContainerDataYaml.getYamlForContainerType(
            kvData.getContainerType());
    kvData.computeAndSetChecksum(yaml);

    return kvData;
  }

  /**
   * @param kvData The container data that will be tested to see if it has
   * metadata values matching those in the database under test.
   */
  private void checkContainerData(KeyValueContainerData kvData) {
    assertEquals(OzoneConsts.SCHEMA_V1, kvData.getSchemaVersion());
    assertEquals(db.KEY_COUNT, kvData.getKeyCount());
    assertEquals(db.BYTES_USED, kvData.getBytesUsed());
    assertEquals(db.NUM_PENDING_DELETION_BLOCKS,
            kvData.getNumPendingDeletionBlocks());
  }

  private int countDeletedBlocks(ReferenceCountedDB refCountedDB)
          throws IOException {
    return refCountedDB.getStore().getDeletedBlocksTable()
            .getRangeKVs(null, 100,
                    MetadataKeyFilters.getDeletedKeyFilter()).size();
  }

  private int countDeletingBlocks(ReferenceCountedDB refCountedDB)
          throws IOException {
    return refCountedDB.getStore().getBlockDataTable()
            .getRangeKVs(null, 100,
                    MetadataKeyFilters.getDeletingKeyFilter()).size();
  }

  private int countUnprefixedBlocks(ReferenceCountedDB refCountedDB)
          throws IOException {
    return refCountedDB.getStore().getBlockDataTable()
            .getRangeKVs(null, 100,
                    MetadataKeyFilters.getNormalKeyFilter()).size();
  }

  /**
   * Holds information about the database used for testing by this class.
   */
  private class TestDB {
    // Non configurable properties of the files.
    public static final long CONTAINER_ID = 123;
    public static final String CONTAINER_FILE_NAME =
            CONTAINER_ID + ".container";
    public static final String DB_NAME = CONTAINER_ID + "-dn-container.db";

    public static final long KEY_COUNT = 2;
    public static final long BYTES_USED = 600;

    public static final long NUM_PENDING_DELETION_BLOCKS = 2;
    public static final long NUM_DELETED_BLOCKS = 2;

    private final ClassLoader loader;

    TestDB() {
      loader = getClass().getClassLoader();
    }

    private File getContainerFile() {
      return new File(loader.getResource(CONTAINER_FILE_NAME)
              .getFile());
    }

    private File getDBDirectory() {
      return new File(loader.getResource(DB_NAME).getFile());
    }
  }
}