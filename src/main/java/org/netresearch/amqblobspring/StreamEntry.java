package org.netresearch.amqblobspring;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

class StreamEntry implements BlobEntry {
  private final String id;
  private final InputStream stream;
  private final Consumer<StreamEntry> onClose;
  private final AtomicBoolean done = new AtomicBoolean(false);

  StreamEntry(String id, long ttl, InputStream stream, Consumer<StreamEntry> onClose) {
    this.id = id;
    this.stream = new CloseAwareInputStream(stream);
    this.onClose = onClose;
    new Timer().schedule(new TimerTask() {
      @Override
      public void run() {
        close();
      }
    }, ttl * 1000);
  }

  private void close() {
    if (done.compareAndSet(false, true)) {
      onClose.accept(StreamEntry.this);
    }
  }

  @Override
  public boolean hasId(String id) {
    return this.id.equals(id);
  }

  @Override
  public InputStream getInputStream() {
    return stream;
  }

  @Override
  public long getContentLength() {
    return -1;
  }

  private class CloseAwareInputStream extends FilterInputStream {
    CloseAwareInputStream(InputStream in) {
      super(in);
    }

    @Override
    public void close() throws IOException {
      try {
        super.close();
      } finally {
        StreamEntry.this.close();
      }
    }
  }
}
