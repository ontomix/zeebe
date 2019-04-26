/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.distributedlog.impl.replication;

import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Namespaces;

/** Storage serializer namespaces. */
public final class LogReplicationNameSpace {

  /** Raft protocol namespace. */
  public static final Namespace LOG_NAME_SPACE =
      Namespace.builder()
          .register(Namespaces.BASIC)
          .register(LogReplicationManifestResponse.class)
          .register(LogReplicationSegmentResponse.class)
          .register(LogReplicationSegmentRequest.class)
          .register(LogReplicationManifestRequest.class)
          .build("RaftProtocol");
}
