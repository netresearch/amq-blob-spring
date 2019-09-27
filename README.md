# ActiveMQ blob message file controller

This is a small library to easily produce ActiveMQ blob messages, serve their blobs with a 
Spring Web controller and delete them when appropriate.

## Installation

See the [instructions on JitPack](https://jitpack.io/#netresearch/amq-blob-spring).

## Usage

Use the `BlobRegistry` provided as `@Service` to create blob messages or byte messages when the content length 
is below the `amq.blob.min` threshold or `amq.blob.enabled` is `false` (**which is the default**).

```java
@Component
public class Sender {
  private final ActiveMQSession session;
  
  private final BlobRegistry blobRegistry;
  
  private final MessageProducer producer;
  
  private final Path file = Paths.get("/tmp/some-tmp-file");
  
  private final byte[] contents = "Some contents".getBytes();
  
  public Sender(ActiveMQSession session, BlobRegistry blobRegistry) throws JMSException {
    this.session = session;
    this.blobRegistry = blobRegistry;
    producer = session.createProducer(session.createQueue("foo"));
  }
  
  public void sendTemporaryFile() throws JMSException, IOException {
    // The file will be deleted after it was downloaded once
    // or no download started before `amq.blob.ttl` from creating the message
    // or immediately if the file size is below `amq.blob.min`
    // (in that case a ByteMessage with the contents of the file will be created)
    producer.send(blobRegistry.createMessage(session, file));
  }
  
  public void sendBytes() throws JMSException, IOException {
    // If the content length is bigger than `amq.blob.ttl` it will be dumped to a
    // temporary file in `amq.blob.dir` which will be deleted after it was downloaded once
    // or no download started before `amq.blob.ttl` from creating the message
    // (Otherwise a BytesMessage will be created)
    producer.send(blobRegistry.createMessage(session, contents));
  }
  
  public void sendForMultipleRetrievals() throws JMSException, IOException {
    // You can specify the expected number of retrievals explicitly
    producer.send(blobRegistry.createMessage(session, file, 42));
    
    // Also for files, the number of expected downloads will be adjusted automatically
    // (in case of sending byte[] this isn't possible and a new file would be created) 
    // (after the next invocation, 43 downloads are expected)
    producer.send(blobRegistry.createMessage(session, file));
  }
  
  public void sendInputStream() throws JMSException {
    // You can also send an input stream - but currently only for one retrieval
    // The stream will be closed after it's sent
    InputStream inputStream = new ByteArrayInputStream(contents);
    producer.send(blobRegistry.createMessage(session, inputStream));
  }
}
```

## Configuration

The following properties are available:

Property | Default | Description
--- | --- | ---
**amq.blob.enabled** | false | If the creation of BlobMessages is enabled at all
amq.blob.min | 1048576 (1MB) | Treshold of content length from which BlobMessages should be created
amq.blob.ttl | 300 (5 minutes) | Number of seconds to wait for downloads to start until the file will be deleted.
amq.blob.dir | java.io.tmpdir | Directory in which to create temporary files when sending creating messages from bytes
amq.blob.url | http://localhost:${server.port} | The URL that should be used as base URL for the blobs

## Caveats

- Currently the underlying `FileInputStream` or other `InputStream` objects will be closed and unregistered also when an
  exception occurs during retrieval. Related files will also be deleted in that case (files messages where created from
  and temporary files automatically created for messages from `Byte[]`)
- Messages created from `InputStream` can only be sent once and without a Content-Length header  