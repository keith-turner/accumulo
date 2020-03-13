package org.apache.accumulo.tserver.compactions;

import java.net.URI;
import java.util.Map;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.spi.compaction.FileInfo;

public interface Compactable {

  TableId getTableId();

  Map<URI,FileInfo> getFiles();

  KeyExtent getExtent();

}
