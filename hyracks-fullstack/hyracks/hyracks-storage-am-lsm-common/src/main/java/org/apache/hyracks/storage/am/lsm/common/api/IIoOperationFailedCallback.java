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
package org.apache.hyracks.storage.am.lsm.common.api;

/**
 * A callback that will be called on failures of IO operations
 */
public interface IIoOperationFailedCallback {
    /**
     * Called on a failed IO operation
     *
     * @param operation
     *            the failed operation
     * @param failure
     *            the failure
     */
    void operationFailed(ILSMIOOperation operation, Throwable failure);

    /**
     * Called on a scheduler failure.
     *
     * @param scheduler
     *            the failed scheduler
     * @param failure
     *            the failure
     */
    void schedulerFailed(ILSMIOOperationScheduler scheduler, Throwable failure);
}
