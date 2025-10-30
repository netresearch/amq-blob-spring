package org.netresearch.amqblobspring;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import jakarta.jms.BytesMessage;
import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.Queue;
import jakarta.jms.QueueReceiver;
import jakarta.jms.Session;
import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(SpringRunner.class)
@SpringBootTest(properties = {
  // "amq.blob.enabled=false", // Disabled by default
  "amq.blob.ttl=2", // 2 seconds
  "amq.blob.min=10" // 10 bytes
})
@ContextConfiguration(initializers = BlobTestConfiguration.Initializer.class)
public class BlobDisabledTest {
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

    assertFalse(Files.exists(file));

    QueueReceiver receiver = session.createReceiver(destination);
    Message message = receiver.receive(500);
    assertTrue(message instanceof BytesMessage);
    assertEquals(content, getContents(message));
  }

  @Test
  public void testBytesMessageCreatedForContentLengthBelowMin() throws Exception {
    Queue destination = session.createQueue("test");

    String content = "To short";
    Path file = testPath.resolve(UUID.randomUUID().toString());
    Files.write(file, content.getBytes());
    session.createProducer(destination).send(registry.createMessage(session, file, 1));

    assertFalse(Files.exists(file));

    QueueReceiver receiver = session.createReceiver(destination);
    Message message = receiver.receive(500);
    assertTrue(message instanceof BytesMessage);
    assertEquals(content, getContents(message));
  }

  @Test
  public void testSingleBlobMessageFromBytes() throws Exception {

    Queue destination = session.createQueue("test");

    String content = "Test content";
    session.createProducer(destination).send(registry.createMessage(session, content.getBytes()));

    QueueReceiver receiver = session.createReceiver(destination);
    Message message = receiver.receive(500);

    assertTrue(message instanceof BytesMessage);
    assertEquals(content, getContents(message));
  }

  @Test
  public void testBytesMessageCreatedForContentLengthBelowMinFromBytes() throws Exception {
    Queue destination = session.createQueue("test");

    String content = "To short";
    session.createProducer(destination).send(registry.createMessage(session, content.getBytes(), 1));

    QueueReceiver receiver = session.createReceiver(destination);
    Message message = receiver.receive(500);

    assertTrue(message instanceof BytesMessage);
    assertEquals(content, getContents(message));
  }

  @Test
  public void testBlobMessageFromInputStream() throws Exception {
    Queue destination = session.createQueue("test");

    String content = "To short"; // But anyway it'll be a BlobMessage
    session.createProducer(destination).send(registry.createMessage(session, new ByteArrayInputStream(content.getBytes())));

    QueueReceiver receiver = session.createReceiver(destination);
    Message message = receiver.receive(500);

    assertTrue(message instanceof BytesMessage);
    assertEquals(content, getContents(message));
  }

  private String getContents(Message message) throws JMSException {
    if (!(message instanceof BytesMessage)) {
      fail("Expected byte message");
    }
    BytesMessage byteMessage = (BytesMessage) message;
    byte[] byteData = new byte[(int) byteMessage.getBodyLength()];
    byteMessage.readBytes(byteData);
    byteMessage.reset();
    return new String(byteData);
  }
}