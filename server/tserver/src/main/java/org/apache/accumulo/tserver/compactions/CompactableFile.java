package org.apache.accumulo.tserver.compactions;

import java.net.URI;

public interface CompactableFile {
  public URI getUri();

  public long getEstimatedSize();

  public long getEstimatedEntries();

  static CompactableFile create(URI uri, long estimatedSize, long estimatedEntries) {
    return new CompactableFileImpl(uri, estimatedSize, estimatedEntries);
  }

}
