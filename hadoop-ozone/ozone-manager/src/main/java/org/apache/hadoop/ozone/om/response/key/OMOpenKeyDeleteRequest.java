/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om.response.key;

import com.google.common.base.Optional;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.key.OMKeyRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OpenKeyBucket;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OpenKey;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.HashMap;
import java.util.List;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

/**
 * Handles requests to move open keys from the open key table to the delete
 * table. Modifies the open key table cache only, and no underlying databases.
 * The delete table cache does not need to be modified since it is not used
 * for client response validation.
 */
public class OMOpenKeyDeleteRequest extends OMKeyRequest {

  private static final Logger LOG =
          LoggerFactory.getLogger(OMOpenKeyDeleteRequest.class);

  public OMOpenKeyDeleteRequest(OMRequest omRequest) {
    super(omRequest);
  }

  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumOpenKeyDeleteRequests();

    OzoneManagerProtocolProtos.DeleteOpenKeysRequest deleteOpenKeysRequest =
            getOmRequest().getDeleteOpenKeysRequest();

    List<OpenKeyBucket> submittedOpenKeyBucket =
            deleteOpenKeysRequest.getOpenKeysPerBucketList();

    long numSubmittedOpenKeys = submittedOpenKeyBucket.stream()
        .mapToLong(OpenKeyBucket::getKeysCount).sum();

    LOG.debug("{} open keys submitted for deletion.", numSubmittedOpenKeys);
    omMetrics.incNumOpenKeysSubmittedForDeletion(numSubmittedOpenKeys);

    OzoneManagerProtocolProtos.OMResponse.Builder omResponse =
            OmResponseUtil.getOMResponseBuilder(getOmRequest());

    IOException exception = null;
    OMClientResponse omClientResponse = null;
    Result result = null;
    Map<String, OmKeyInfo> deletedOpenKeys = new HashMap<>();

    try {
      // Open keys are grouped by bucket, but there may be multiple buckets
      // per volume. This maps volume name to volume args to track
      // all volume updates for this request.
      Map<String, OmVolumeArgs> modifiedVolumes = new HashMap<>();
      OMMetadataManager metadataManager = ozoneManager.getMetadataManager();

      for (OpenKeyBucket openKeyBucket: submittedOpenKeyBucket) {
        // For each bucket where keys will be deleted from,
        // get its bucket lock and update the cache accordingly.
        Map<String, OmKeyInfo> deleted = updateOpenKeyTableCache(ozoneManager,
            trxnLogIndex, openKeyBucket);

        deletedOpenKeys.putAll(deleted);

        // If open keys were deleted from this bucket and its volume still
        // exists, update the volume's byte usage in the cache.
        if (!deleted.isEmpty()) {
          String volumeName = openKeyBucket.getVolumeName();
          // Returns volume args from the cache if the volume is present,
          // null otherwise.
          OmVolumeArgs volumeArgs = getVolumeInfo(metadataManager, volumeName);

          // If this volume still exists, decrement bytes used based on open
          // keys deleted.
          // The volume args object being updated is a reference from the
          // cache, so this serves as a cache update.
          if (volumeArgs != null) {
            // If we already encountered the volume, it was a reference to
            // the same object from the cache, so this will update it.
            modifiedVolumes.put(volumeName, volumeArgs);
            subtractUsedBytes(volumeArgs, deleted.values());
          }
        }
      }

      omClientResponse = new OMOpenKeyDeleteResponse(omResponse.build(),
          deletedOpenKeys, ozoneManager.isRatisEnabled(),
          modifiedVolumes.values());

      result = Result.SUCCESS;
    } catch (IOException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse =
          new OMKeyDeleteResponse(createErrorOMResponse(omResponse, exception));
    } finally {
      addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
              omDoubleBufferHelper);
    }

    processResults(omMetrics, numSubmittedOpenKeys, deletedOpenKeys.size(),
        deleteOpenKeysRequest, result);

    return omClientResponse;
  }

  private void processResults(OMMetrics omMetrics, long numSubmittedOpenKeys,
      long numDeletedOpenKeys,
      OzoneManagerProtocolProtos.DeleteOpenKeysRequest request, Result result) {

    switch (result) {
    case SUCCESS:
      LOG.debug("Deleted {} open keys out of {} submitted keys.",
          numDeletedOpenKeys, numSubmittedOpenKeys);
      break;
    case FAILURE:
      omMetrics.incNumOpenKeyDeleteRequestFails();
      LOG.error("Failure occurred while trying to delete {} submitted open " +
              "keys.", numSubmittedOpenKeys);
      break;
    default:
      LOG.error("Unrecognized result for OMOpenKeyDeleteRequest: {}",
          request);
    }
  }

  private Map<String, OmKeyInfo> updateOpenKeyTableCache(
      OzoneManager ozoneManager, long trxnLogIndex, OpenKeyBucket keysPerBucket)
      throws IOException {

    Map<String, OmKeyInfo> deletedKeys = new HashMap<>();

    boolean acquiredLock = false;
    String volumeName = keysPerBucket.getVolumeName();
    String bucketName = keysPerBucket.getBucketName();
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    try {
      acquiredLock = omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK,
              volumeName, bucketName);

      for (OpenKey key: keysPerBucket.getKeysList()) {
        String fullKeyName = omMetadataManager.getOpenKey(volumeName,
                bucketName, key.getName(), key.getClientID());

        // If an open key is no longer present in the table, it was committed
        // and should not be deleted.
        OmKeyInfo omKeyInfo =
            omMetadataManager.getOpenKeyTable().get(fullKeyName);
        if (omKeyInfo != null) {
          // Set the UpdateID to current transactionLogIndex
          omKeyInfo.setUpdateID(trxnLogIndex, ozoneManager.isRatisEnabled());
          deletedKeys.put(fullKeyName, omKeyInfo);

          // Update table cache.
          omMetadataManager.getOpenKeyTable().addCacheEntry(
                  new CacheKey<>(fullKeyName),
                  new CacheValue<>(Optional.absent(), trxnLogIndex));

          ozoneManager.getMetrics().incNumOpenKeysDeleted();
          LOG.debug("Open key {} deleted.", fullKeyName);

          // No need to add cache entries to delete table. As delete table will
          // be used by DeleteKeyService only, not used for any client response
          // validation, so we don't need to add to cache.
        } else {
          LOG.debug("Key {} was not deleted, as it was not " +
                  "found in the open key table.", fullKeyName);
        }
      }
    } finally {
      if (acquiredLock) {
        omMetadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName,
                bucketName);
      }
    }

    return deletedKeys;
  }

  /**
   * Subtracts all bytes used by the blocks pointed to by {@code keyInfos}
   * from {@code volumeArgs}.
   */
  private void subtractUsedBytes(OmVolumeArgs volumeArgs,
      Collection<OmKeyInfo> keyInfos) {

    long quotaReleased = keyInfos.stream()
        .mapToLong(OMOpenKeyDeleteRequest::getUsedBytes)
        .sum();

    // update usedBytes atomically.
    volumeArgs.getUsedBytes().add(-quotaReleased);
  }
}
