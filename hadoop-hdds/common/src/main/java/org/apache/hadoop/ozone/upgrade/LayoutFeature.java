/**
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

package org.apache.hadoop.ozone.upgrade;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeDataView;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import java.util.Collections;
import java.util.Optional;

/**
 * Generic Layout feature interface for Ozone.
 */
public interface LayoutFeature extends CompositeDataView {
  String name();

  int layoutVersion();

  String description();

  @Override
  default CompositeData toCompositeData(CompositeType ct) {
    try {
      return new CompositeDataSupport(ct, Collections.EMPTY_MAP);
    }
    catch (OpenDataException e) {
      throw new RuntimeException(e);
    }
  }

  default Optional<? extends UpgradeAction> onFinalizeAction() {
    return Optional.empty();
  }

  /**
   * Generic UpgradeAction interface. An operation that is run on specific
   * upgrade states like post finalize, pre-downgrade etc.
   * @param <T>
   */
  interface UpgradeAction<T> {
    void executeAction(T arg) throws Exception;
  }
}
