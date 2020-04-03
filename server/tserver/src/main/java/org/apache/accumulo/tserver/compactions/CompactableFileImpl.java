package org.apache.accumulo.tserver.compactions;

import java.net.URI;

import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;

public class CompactableFileImpl implements CompactableFile {

  private final StoredTabletFile storedTabletFile;
  private final DataFileValue dataFileValue;

  public CompactableFileImpl(URI uri, long size, long entries) {
    this.storedTabletFile = new StoredTabletFile(uri.toString());
    this.dataFileValue = new DataFileValue(size, entries);
  }

  public CompactableFileImpl(StoredTabletFile storedTabletFile, DataFileValue dataFileValue) {
    this.storedTabletFile = storedTabletFile;
    this.dataFileValue = dataFileValue;
  }

  @Override
  public URI getUri() {
    return storedTabletFile.getPath().toUri();
  }

  @Override
  public long getEstimatedSize() {
    return dataFileValue.getSize();
  }

  @Override
  public long getEstimatedEntries() {
    return dataFileValue.getNumEntries();
  }

  public StoredTabletFile getStortedTabletFile() {
    return storedTabletFile;
  }

  public boolean equals(Object o) {
    if (o instanceof CompactableFileImpl) {
      var ocfi = (CompactableFileImpl) o;

      return storedTabletFile.equals(ocfi.storedTabletFile);
    }

    return false;
  }
}
