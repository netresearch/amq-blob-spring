package org.netresearch.amqblobspring;

import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.blob.BlobTransferPolicy;
import org.apache.activemq.blob.BlobUploader;
import org.apache.activemq.command.ActiveMQBlobMessage;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.jms.JMSException;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

@Service
@SuppressWarnings("WeakerAccess")
public class BlobRegistry {
  @Value("${amq.blob.ttl:300}")
  private long ttl;

  @Value("${amq.blob.url:http://localhost:${server.port}}")
  private URI url;

  private final Collection<BlobEntry> files = new CopyOnWriteArrayList<>();

  BlobEntry getFileEntry(String id) {
    return files.stream().filter(blobEntry -> blobEntry.hasId(id)).findFirst().orElse(null);
  }

  private BlobEntry getFileEntry(Path path) {
    return files.stream().filter(fe -> fe.getPath().equals(path)).findFirst().orElse(null);
  }

  public ActiveMQBlobMessage createBlobMessage(ActiveMQSession session, Path path, int expectedDownloads)
      throws JMSException {
    BlobEntry blobEntry = getFileEntry(path);
    if (blobEntry == null) {
      blobEntry = new BlobEntry(path, ttl, files::remove);
      files.add(blobEntry);
    }
    String id = UUID.randomUUID().toString();
    URL fileUrl;
    try {
      fileUrl = url.resolve("/blob/" + id).toURL();
    } catch (MalformedURLException e) {
      throw new JMSException("Error while creating the URL");
    }
    ActiveMQBlobMessage message = (ActiveMQBlobMessage) session.createBlobMessage(fileUrl);
    message.setBlobUploader(new NoopUploader(message));
    blobEntry.expectDownloads(id, expectedDownloads);
    return message;
  }

  private static final class NoopUploader extends BlobUploader {
    private final ActiveMQBlobMessage message;

    NoopUploader(ActiveMQBlobMessage blobMessage) {
      super(new BlobTransferPolicy(), (File) null);
      this.message = blobMessage;
    }

    @Override
    public URL upload(ActiveMQBlobMessage message) throws JMSException {
      if (!message.equals(this.message)) {
        throw new JMSException("Wrong message");
      }
      return message.getURL();
    }
  }
}
