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
package io.zeebe.gateway.api.job;

import io.zeebe.gateway.api.util.StubbedGateway;
import io.zeebe.gateway.api.util.StubbedGateway.RequestStub;
import io.zeebe.gateway.impl.broker.request.BrokerUpdateJobRetriesRequest;
import io.zeebe.gateway.impl.broker.response.BrokerResponse;
import io.zeebe.protocol.impl.record.value.job.JobRecord;

public class UpdateJobRetriesStub extends JobRequestStub
    implements RequestStub<BrokerUpdateJobRetriesRequest, BrokerResponse<JobRecord>> {

  @Override
  public void registerWith(StubbedGateway gateway) {
    gateway.registerHandler(BrokerUpdateJobRetriesRequest.class, this);
  }

  @Override
  public BrokerResponse<JobRecord> handle(BrokerUpdateJobRetriesRequest request) throws Exception {
    final JobRecord value = buildDefaultValue();
    value.setRetries(request.getRequestWriter().getRetries());

    return new BrokerResponse<>(value, 0, request.getKey());
  }
}
