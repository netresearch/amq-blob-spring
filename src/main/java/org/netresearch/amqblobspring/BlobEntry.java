package org.netresearch.amqblobspring;

import java.io.InputStream;

interface BlobEntry {
  InputStream getInputStream();

  long getContentLength();

  boolean hasId(String id);
}
