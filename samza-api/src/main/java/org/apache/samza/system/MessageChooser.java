/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.system;

/**
 * MessageChooser is an interface for programmatic fine-grain control over
 * stream consumption.
 * 
 * Consider the case of a Samza task is consuming multiple streams where some
 * streams may be from lived systems that have stricter SLA requirements and
 * must always be prioritized over other streams that may be from batch systems.
 * MessageChooser allows developers to inject message prioritization logic into
 * the SamzaContainer.
 * 
 * In general, the MessageChooser can be used to prioritize certain systems,
 * streams or partitions over others. It can also be used to throttle certain
 * partitions if it chooses not to return messages even though they are
 * available when choose is invoked. The MessageChooser can also throttle the
 * entire SamzaContainer by performing a blocking operation, such as
 * Thread.sleep.
 * 
 * The contract between the MessageChooser and the SystemConsumers is:
 * 
 * <ul>
 * <li>Update can be called multiple times before choose is called.</li>
 * <li>A null return from MessageChooser.choose means no envelopes should be
 * processed at the moment.</li>
 * <li>A MessageChooser may elect to return null when choose is called, even if
 * unprocessed messages have been given by the update method.</li>
 * <li>A MessageChooser will not have any of its in-memory state restored in the
 * event of a failure.</li>
 * <li>Blocking operations (such as Thread.sleep) will block all processing in
 * the entire SamzaContainer.</li>
 * <li>A MessageChooser should never return the same envelope more than once.</li>
 * <li>Non-deterministic (e.g. time-based) MessageChoosers are allowed.</li>
 * <li>A MessageChooser does not need to be thread-safe.</li>
 * </ul>
 */
public interface MessageChooser {
  /**
   * Called after all SystemStreamPartitions have been registered. Start is used
   * to notify the chooser that it will start receiving update and choose calls.
   */
  void start();

  /**
   * Called when the chooser is about to be discarded. No more messages will be
   * given to the chooser after it is stopped.
   */
  void stop();

  /**
   * Called before start, to let the chooser know that it will be handling
   * envelopes from the given SystemStreamPartition. Register will only be
   * called before start.
   * 
   * @param systemStreamPartition
   *          A SystemStreamPartition that envelopes will be coming from.
   */
  void register(SystemStreamPartition systemStreamPartition);

  /**
   * Notify the chooser that a new envelope is available for a processing.A
   * MessageChooser will receive, at most, one outstanding envelope per
   * system/stream/partition combination. For example, if update is called for
   * partition 7 of kafka.mystream, then update will not be called with an
   * envelope from partition 7 of kafka.mystream until the previous envelope has
   * been returned via the choose method. Update will only be invoked after the
   * chooser has been started.
   * 
   * @param envelope
   *          An unprocessed envelope.
   */
  void update(IncomingMessageEnvelope envelope);

  /**
   * The choose method is invoked when the SamzaContainer is ready to process a
   * new message. The chooser may elect to return any envelope that it's been
   * given via the update method, which hasn't yet been returned. Choose will
   * only be called after the chooser has been started.
   * 
   * @return The next envelope to process, or null if the chooser has no
   *         messages or doesn't want to process any at the moment.
   */
  IncomingMessageEnvelope choose();
}
