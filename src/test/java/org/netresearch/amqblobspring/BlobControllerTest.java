package org.netresearch.amqblobspring;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.command.ActiveMQBlobMessage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.StreamUtils;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueReceiver;
import javax.jms.Session;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT, properties = {
  "amq.blob.enabled=true",
  "amq.blob.ttl=2", // 2 seconds
  "amq.blob.min=10" // 10 bytes
})
@EnableWebMvc
@ContextConfiguration(initializers = BlobTestConfiguration.Initializer.class)
public class BlobControllerTest {
  @Value("nio://localhost:${jmsPort}")
  private String amqUrl;

  @Value("${java.io.tmpdir}")
  private Path testPath;

  @Autowired
  private BlobRegistry registry;

  private Connection connection;
  private ActiveMQSession session;

  @Before
  public void setUp() throws Exception {
    connection = new ActiveMQConnectionFactory(amqUrl).createConnection();
    connection.start();
    session = (ActiveMQSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
  }

  @After
  public void tearDown() throws Exception {
    if (connection != null) {
      connection.close();
    }
  }

  @Test
  public void testSingleBlobMessageFromFile() throws Exception {
    Queue destination = session.createQueue("test");

    String content = "Test content";
    Path file = testPath.resolve(UUID.randomUUID().toString());
    Files.write(file, content.getBytes());
    session.createProducer(destination).send(registry.createMessage(session, file, 1));

    QueueReceiver receiver = session.createReceiver(destination);
    Message message = receiver.receive(500);
    assertTrue(message instanceof ActiveMQBlobMessage);
    assertTrue(Files.exists(file));
    assertEquals(content, StreamUtils.copyToString(((ActiveMQBlobMessage) message).getInputStream(), StandardCharsets.UTF_8));

    // The final deletion happens asynchronously and might be a bit delayed
    runWithDelay(10, () -> assertFalse(Files.exists(file)));
  }

  @Test
  public void testMultipleBlobMessagesFromFile() throws Exception {
    Queue destination = session.createQueue("test");
    String content = "Test content";
    Path file = testPath.resolve(UUID.randomUUID().toString());
    Files.write(file, content.getBytes());
    MessageProducer producer = session.createProducer(destination);
    QueueReceiver receiver = session.createReceiver(destination);

    for (int i = 1; i <=3; i++) {
      producer.send(registry.createMessage(session, file, 1));
    }

    for (int i = 1; i <=3; i++) {
      Message message = receiver.receive(500);
      assertTrue(message instanceof ActiveMQBlobMessage);
      assertTrue(Files.exists(file));
      assertEquals(content, StreamUtils.copyToString(((ActiveMQBlobMessage) message).getInputStream(), StandardCharsets.UTF_8));
    }

    // The final deletion happens asynchronously and might be a bit delayed
    runWithDelay(10, () -> assertFalse(Files.exists(file)));
  }


  @Test
  public void testNotFetchedFilesDeletedAfterTtl() throws Exception {
    Queue destination = session.createQueue("test");

    Path file = testPath.resolve(UUID.randomUUID().toString());
    Files.write(file, "Test content".getBytes());
    session.createProducer(destination).send(registry.createMessage(session, file, 1));
    session.createProducer(destination).send(registry.createMessage(session, file));

    QueueReceiver receiver = session.createReceiver(destination);
    receiver.receive(500);

    // The final deletion happens asynchronously and might be a bit delayed
    runWithDelay(10, () -> assertTrue(Files.exists(file)));

    runWithDelay(2100, () -> assertFalse(Files.exists(file)));

    Message lastMessage = receiver.receive();
    try {
      StreamUtils.copyToString(((ActiveMQBlobMessage) lastMessage).getInputStream(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      assertTrue(e.getMessage().startsWith("Server returned HTTP response code: 403"));
    }
  }

  @Test
  public void testBytesMessageCreatedForContentLengthBelowMin() throws Exception {
    Queue destination = session.createQueue("test");

    String content = "To short";
    Path file = testPath.resolve(UUID.randomUUID().toString());
    Files.write(file, content.getBytes());
    session.createProducer(destination).send(registry.createMessage(session, file, 1));

    QueueReceiver receiver = session.createReceiver(destination);
    Message message = receiver.receive(500);
    assertTrue(message instanceof BytesMessage);
    assertFalse(Files.exists(file));

    BytesMessage byteMessage = (BytesMessage) message;
    byte[] byteData = new byte[(int) byteMessage.getBodyLength()];
    byteMessage.readBytes(byteData);
    byteMessage.reset();

    assertEquals(content, new String(byteData, Charset.defaultCharset()));
  }

  @Test
  public void testSingleBlobMessageFromBytes() throws Exception {

    Queue destination = session.createQueue("test");

    String content = "Test content";
    session.createProducer(destination).send(registry.createMessage(session, content.getBytes()));

    QueueReceiver receiver = session.createReceiver(destination);
    Message message = receiver.receive(500);
    assertTrue(message instanceof ActiveMQBlobMessage);
    String url = ((ActiveMQBlobMessage) message).getRemoteBlobUrl();
    Path file = testPath.resolve(url.substring(url.lastIndexOf("/") + 1));
    assertTrue(Files.exists(file));
    assertEquals(content, StreamUtils.copyToString(((ActiveMQBlobMessage) message).getInputStream(), StandardCharsets.UTF_8));

    // The final deletion happens asynchronously and might be a bit delayed
    runWithDelay(10, () -> assertFalse(Files.exists(file)));
  }

  @Test
  public void testBytesMessageCreatedForContentLengthBelowMinFromBytes() throws Exception {
    Queue destination = session.createQueue("test");

    String content = "To short";
    session.createProducer(destination).send(registry.createMessage(session, content.getBytes(), 1));

    QueueReceiver receiver = session.createReceiver(destination);
    Message message = receiver.receive(500);
    assertTrue(message instanceof BytesMessage);

    BytesMessage byteMessage = (BytesMessage) message;
    byte[] byteData = new byte[(int) byteMessage.getBodyLength()];
    byteMessage.readBytes(byteData);
    byteMessage.reset();

    assertEquals(content, new String(byteData, Charset.defaultCharset()));
  }

  @Test
  public void testBlobMessageFromInputStream() throws Exception {
    Queue destination = session.createQueue("test");

    String content = "To short"; // But anyway it'll be a BlobMessage
    session.createProducer(destination).send(registry.createMessage(session, new ByteArrayInputStream(content.getBytes())));

    QueueReceiver receiver = session.createReceiver(destination);
    Message message = receiver.receive(500);
    assertTrue(message instanceof ActiveMQBlobMessage);
    assertEquals(content, StreamUtils.copyToString(((ActiveMQBlobMessage) message).getInputStream(), StandardCharsets.UTF_8));

    // Assert that StreamEntry is removed after retrieval
    String url = ((ActiveMQBlobMessage) message).getRemoteBlobUrl();
    assertNull(registry.getEntry(url.substring(url.lastIndexOf("/") + 1)));
  }

  private void runWithDelay(long delay, Runnable task) throws InterruptedException {
    Thread.sleep(delay);
    task.run();
  }
}