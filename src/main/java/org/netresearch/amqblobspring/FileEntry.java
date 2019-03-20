package org.netresearch.amqblobspring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

class FileEntry implements BlobEntry {
  private static final Logger log = LoggerFactory.getLogger(FileEntry.class);

  private final Path path;

  private final Collection<String> ids = new CopyOnWriteArraySet<>();
  private final long ttl;
  private final Consumer<FileEntry> onDeletion;
  private final AtomicInteger expectedDownloads = new AtomicInteger(0);
  private final AtomicInteger doneDownloads = new AtomicInteger(0);
  private final AtomicInteger runningDownloads = new AtomicInteger(0);
  private final AtomicBoolean deleted = new AtomicBoolean(false);

  private final AtomicReference<Timer> timer = new AtomicReference<>();

  FileEntry(Path path, long ttl, Consumer<FileEntry> onDeletion) {
    this.path = path;
    this.ttl = ttl;
    this.onDeletion = onDeletion;
  }

  private void scheduleDeletion() {
    unscheduleDeletion();
    final int expected = expectedDownloads.get();
    timer.set(new Timer());
    timer.get().schedule(new TimerTask() {
      @Override
      public void run() {
        if (expected == expectedDownloads.get() && runningDownloads.get() == 0 && !deleted.get()) {
          close(true);
        }
      }
    }, ttl * 1000, ttl * 1000);
  }

  private void unscheduleDeletion() {
    if (timer.get() != null) {
      timer.getAndSet(null).cancel();
    }
  }

  void expectDownloads(String id, int expectedDownloads) {
    ids.add(id);
    this.expectedDownloads.addAndGet(expectedDownloads);
    this.scheduleDeletion();
  }

  @Override
  public boolean hasId(String id) {
    return ids.contains(id);
  }

  Path getPath() {
    return path;
  }

  @Override
  public InputStream getInputStream() {
    InputStream stream = null;
    try {
      stream = new DeleteOnCloseFileInputStream();
    } catch (FileNotFoundException e) {
      close(false);
    }
    return stream;
  }

  @Override
  public long getContentLength() {
    return path.toFile().length();
  }

  private void close(boolean delete) {
    if (deleted.get()) {
      return;
    }
    deleted.set(true);
    try {
      if (delete) {
        Files.delete(path);
      }
      onDeletion.accept(this);
      unscheduleDeletion();
      log.info("Deleted {}", path);
    } catch (IOException e) {
      log.error("Error while deleting {}", path, e);
      deleted.set(false);
    }
  }

  class DeleteOnCloseFileInputStream extends FileInputStream {
    DeleteOnCloseFileInputStream() throws FileNotFoundException {
      super(path.toFile());
      runningDownloads.incrementAndGet();
    }

    @Override
    public void close() throws IOException {
      try {
        super.close();
      } finally {
        if (doneDownloads.incrementAndGet() == expectedDownloads.get()) {
          FileEntry.this.close(true);
        }
        runningDownloads.decrementAndGet();
      }
    }
  }
}
