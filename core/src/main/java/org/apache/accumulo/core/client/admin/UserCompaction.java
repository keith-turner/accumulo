package org.apache.accumulo.core.client.admin;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.UserCompactionId;

public interface UserCompaction {

  String getTableName();

  TableId getTableId();

  UserCompactionId getId();

  int getTabletsCompacted();

  int getTabletsToCompact();

  CompactionConfig getConfig();
}
