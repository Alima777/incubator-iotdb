/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.memtable;

import java.util.ArrayDeque;
import java.util.Deque;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemTablePool {

  private static final Logger logger = LoggerFactory.getLogger(MemTablePool.class);

  private static final Deque<IMemTable> availableMemTables = new ArrayDeque<>();

  /**
   * >= number of storage group * 2
   * TODO check this parameter to ensure that capaity * MaxMemTable Size < JVM memory / 2
   */
  private static final int capacity = IoTDBDescriptor.getInstance().getConfig().getMemtableNumber();

  private int size = 0;

  private static final int WAIT_TIME = 2000;

  private MemTablePool() {
  }

  public IMemTable getAvailableMemTable(Object applier) {
    synchronized (availableMemTables) {
      if (availableMemTables.isEmpty() && size < capacity) {
        size++;
        logger.info("generated a new memtable for {}, system memtable size: {}, stack size: {}",
            applier, size, availableMemTables.size());
        return new PrimitiveMemTable();
      } else if (!availableMemTables.isEmpty()) {
        logger
            .info("system memtable size: {}, stack size: {}, then get a memtable from stack for {}",
                size, availableMemTables.size(), applier);
        return availableMemTables.pop();
      }

      // wait until some one has released a memtable
      int waitCount = 1;
      while (true) {
        if (!availableMemTables.isEmpty()) {
          logger.info(
              "system memtable size: {}, stack size: {}, then get a memtable from stack for {}",
              size, availableMemTables.size(), applier);
          return availableMemTables.pop();
        }
        try {
          availableMemTables.wait(WAIT_TIME);
        } catch (InterruptedException e) {
          logger.error("{} fails to wait fot memtables {}, continue to wait", applier, e);
        }
        logger.info("{} has waited for a memtable for {}ms", applier, waitCount++ * WAIT_TIME);
      }
    }
  }

  public void putBack(IMemTable memTable, String storageGroup) {
    if (memTable.isSignalMemTable()) {
      return;
    }
    synchronized (availableMemTables) {
      memTable.clear();
      availableMemTables.push(memTable);
      availableMemTables.notify();
      logger.info("{} return a memtable, stack size {}", storageGroup, availableMemTables.size());
    }
  }

  public static MemTablePool getInstance() {
    return InstanceHolder.INSTANCE;
  }

  private static class InstanceHolder {

    private InstanceHolder() {
    }

    private static final MemTablePool INSTANCE = new MemTablePool();
  }
}
