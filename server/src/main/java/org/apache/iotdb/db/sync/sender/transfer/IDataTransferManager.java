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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.sync.sender.transfer;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import org.apache.iotdb.db.exception.SyncConnectionException;
import org.apache.thrift.TException;

/**
 * This interface is used to realize the data transmission part of synchronization task, and is also
 * the most important part of synchronization task. By screening out all transmission files to be
 * synchronized in <class>SyncFileManager</class>, these files are synchronized to the receiving end
 * to complete the synchronization task.
 */
public interface IDataTransferManager {

  void init();

  /**
   * Establish a connection to receiver end.
   */
  void establishConnection(String serverIp, int serverPort) throws SyncConnectionException;

  /**
   * Confirm identity, the receiver will check whether the sender has synchronization privileges.
   */
  void confirmIdentity() throws SyncConnectionException, IOException;

  /**
   * Sync schema file to receiver before all data to be synced.
   */
  void syncSchema() throws SyncConnectionException, TException;

  /**
   * For deleted files in a storage group, sync them to receiver side and load these files in
   * receiver.
   *
   * @param sgName storage group name
   * @param deletedFilesName list of deleted file names
   */
  void syncDeletedFilesNameInOneGroup(String sgName, Set<File> deletedFilesName)
      throws SyncConnectionException, IOException;

  /**
   * Execute a sync task for all data directory.
   */
  void syncAll() throws SyncConnectionException, IOException, TException;

  /**
   * Execute a sync task for a data directory.
   */
  void sync() throws SyncConnectionException, IOException;

  /**
   * For new valid files in a storage group, sync them to receiver side and load these data in
   * receiver.
   *
   * @param sgName storage group name
   * @param toBeSyncFiles list of new tsfile names
   */
  void syncDataFilesInOneGroup(String sgName, Set<File> toBeSyncFiles)
      throws SyncConnectionException, IOException;

  /**
   * Stop sync process
   */
  void stop();

}
