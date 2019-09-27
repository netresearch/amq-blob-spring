package org.netresearch.amqblobspring;

import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.blob.BlobTransferPolicy;
import org.apache.activemq.blob.BlobUploader;
import org.apache.activemq.command.ActiveMQBlobMessage;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.StreamUtils;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

@Service
public class BlobRegistry {
  @Value("${amq.blob.enabled:false}")
  private boolean blobsEnabled;

  @Value("${amq.blob.ttl:300}")
  private long ttl;

  @Value("${amq.blob.url:http://localhost:${server.port:8080}}")
  private URI url;

  @Value("${amq.blob.min:#{1*1024*1024}}")
  private long blobMinLength;

  @Value("${amq.blob.dir:${java.io.tmpdir}}")
  private Path dir;

  private final Collection<BlobEntry> entries = new CopyOnWriteArrayList<>();

  BlobEntry getEntry(String id) {
    return entries.stream().filter(blobEntry -> blobEntry.hasId(id)).findFirst().orElse(null);
  }

  public Message createMessage(ActiveMQSession session, Path path, int expectedDownloads)
      throws JMSException, IOException {
    if (!blobsEnabled || path.toFile().length() <= blobMinLength) {
      BytesMessage message = session.createBytesMessage();
      message.writeBytes(Files.readAllBytes(path));
      Files.delete(path);
      return message;
    }

    return createMessage(session, UUID.randomUUID().toString(), path, expectedDownloads);
  }

  public Message createMessage(ActiveMQSession session, Path path) throws JMSException, IOException {
    return createMessage(session, path, 1);
  }

  public Message createMessage(ActiveMQSession session, byte[] contents) throws JMSException, IOException {
    return createMessage(session, contents, 1);
  }

  public Message createMessage(ActiveMQSession session, byte[] contents, int expectedDownloads) throws JMSException, IOException {
    if (!blobsEnabled || contents.length <= blobMinLength) {
      BytesMessage message = session.createBytesMessage();
      message.writeBytes(contents);
      return message;
    }
    String id = UUID.randomUUID().toString();
    Path path = dir.resolve(id);
    Files.copy(new ByteArrayInputStream(contents), path);
    System.out.println("Send: " + path);
    return createMessage(session, id, path, expectedDownloads);
  }

  public Message createMessage(ActiveMQSession session, InputStream inputStream) throws JMSException {
    if (!blobsEnabled) {
      try {
        try (InputStream in = inputStream; ByteArrayOutputStream out = new ByteArrayOutputStream()) {
          StreamUtils.copy(in, out);
          return createMessage(session, out.toByteArray());
        }
      } catch (IOException e) {
        throw new JMSException("Error while reading the input stream: " + e);
      }
    }
    String id = UUID.randomUUID().toString();
    entries.add(new StreamEntry(id, ttl, inputStream, entries::remove));
    return createMessage(session, id);
  }

  private Message createMessage(ActiveMQSession session, String id, Path path, int expectedDownloads)
      throws JMSException {
    FileEntry fileEntry = (FileEntry) entries.stream().filter(
        fe -> fe instanceof FileEntry && ((FileEntry) fe).getPath().equals(path)
    ).findFirst().orElse(null);

    if (fileEntry == null) {
      fileEntry = new FileEntry(path, ttl, entries::remove);
      entries.add(fileEntry);
    }

    Message message = createMessage(session, id);
    fileEntry.expectDownloads(id, expectedDownloads);
    return message;
  }

  private Message createMessage(ActiveMQSession session, String id) throws JMSException {
    URL fileUrl;
    try {
      fileUrl = url.resolve("/blob/" + id).toURL();
    } catch (MalformedURLException e) {
      throw new JMSException("Error while creating the URL");
    }
    ActiveMQBlobMessage message = (ActiveMQBlobMessage) session.createBlobMessage(fileUrl);
    message.setBlobUploader(new NoopUploader(message));
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
