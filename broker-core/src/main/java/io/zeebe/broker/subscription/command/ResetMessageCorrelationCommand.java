/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.subscription.command;

import static io.zeebe.broker.subscription.ResetMessageCorrelationDecoder.correlationKeyHeaderLength;
import static io.zeebe.broker.subscription.ResetMessageCorrelationDecoder.workflowInstanceKeyNullValue;
import static io.zeebe.broker.subscription.ResetMessageCorrelationEncoder.elementInstanceKeyNullValue;
import static io.zeebe.broker.subscription.ResetMessageCorrelationEncoder.messageNameHeaderLength;
import static io.zeebe.broker.subscription.ResetMessageCorrelationEncoder.subscriptionPartitionIdNullValue;

import io.zeebe.broker.subscription.ResetMessageCorrelationDecoder;
import io.zeebe.broker.subscription.ResetMessageCorrelationEncoder;
import io.zeebe.broker.util.SbeBufferWriterReader;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class ResetMessageCorrelationCommand
    extends SbeBufferWriterReader<ResetMessageCorrelationEncoder, ResetMessageCorrelationDecoder> {

  private final ResetMessageCorrelationEncoder encoder = new ResetMessageCorrelationEncoder();
  private final ResetMessageCorrelationDecoder decoder = new ResetMessageCorrelationDecoder();

  private int subscriptionPartitionId;
  private long workflowInstanceKey;
  private long elementInstanceKey;
  private long messageKey;

  private final UnsafeBuffer messageName = new UnsafeBuffer(0, 0);
  private final UnsafeBuffer correlationKey = new UnsafeBuffer(0, 0);

  @Override
  protected ResetMessageCorrelationEncoder getBodyEncoder() {
    return encoder;
  }

  @Override
  protected ResetMessageCorrelationDecoder getBodyDecoder() {
    return decoder;
  }

  @Override
  public int getLength() {
    return super.getLength()
        + messageNameHeaderLength()
        + messageName.capacity()
        + correlationKeyHeaderLength()
        + correlationKey.capacity();
  }

  @Override
  public void write(MutableDirectBuffer buffer, int offset) {
    super.write(buffer, offset);

    encoder
        .subscriptionPartitionId(subscriptionPartitionId)
        .workflowInstanceKey(workflowInstanceKey)
        .elementInstanceKey(elementInstanceKey)
        .messageKey(messageKey)
        .putMessageName(messageName, 0, messageName.capacity())
        .putCorrelationKey(correlationKey, 0, correlationKey.capacity());
  }

  @Override
  public void wrap(DirectBuffer buffer, int offset, int length) {
    super.wrap(buffer, offset, length);

    subscriptionPartitionId = decoder.subscriptionPartitionId();
    workflowInstanceKey = decoder.workflowInstanceKey();
    elementInstanceKey = decoder.elementInstanceKey();
    messageKey = decoder.messageKey();

    decoder.wrapMessageName(messageName);
    decoder.wrapCorrelationKey(correlationKey);
  }

  @Override
  public void reset() {
    subscriptionPartitionId = subscriptionPartitionIdNullValue();
    workflowInstanceKey = workflowInstanceKeyNullValue();
    elementInstanceKey = elementInstanceKeyNullValue();
    messageKey = ResetMessageCorrelationDecoder.messageKeyNullValue();
    messageName.wrap(0, 0);
    correlationKey.wrap(0, 0);
  }

  public int getSubscriptionPartitionId() {
    return subscriptionPartitionId;
  }

  public void setSubscriptionPartitionId(int subscriptionPartitionId) {
    this.subscriptionPartitionId = subscriptionPartitionId;
  }

  public long getWorkflowInstanceKey() {
    return workflowInstanceKey;
  }

  public void setWorkflowInstanceKey(long workflowInstanceKey) {
    this.workflowInstanceKey = workflowInstanceKey;
  }

  public long getElementInstanceKey() {
    return elementInstanceKey;
  }

  public void setElementInstanceKey(long elementInstanceKey) {
    this.elementInstanceKey = elementInstanceKey;
  }

  public long getMessageKey() {
    return messageKey;
  }

  public void setMessageKey(final long messageKey) {
    this.messageKey = messageKey;
  }

  public DirectBuffer getMessageName() {
    return messageName;
  }

  public DirectBuffer getCorrelationKey() {
    return correlationKey;
  }
}
