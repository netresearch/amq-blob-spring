package org.netresearch.amqblobspring;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQBlobMessage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.core.env.MapPropertySource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.SocketUtils;
import org.springframework.util.StreamUtils;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueReceiver;
import javax.jms.Session;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@SpringBootConfiguration
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT, properties = {
    "amq.blob.ttl=2" // 2 seconds
})
@ContextConfiguration(initializers = BlobControllerTest.Initializer.class)
@EnableAutoConfiguration
@EnableWebMvc
@ComponentScan
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
  public void testActiveMqSingle() throws Exception {
    Queue destination = session.createQueue("test");

    String content = "Test content";
    Path file = testPath.resolve(UUID.randomUUID().toString());
    Files.write(file, content.getBytes());
    session.createProducer(destination).send(registry.createBlobMessage(session, file, 1));

    QueueReceiver receiver = session.createReceiver(destination);
    Message message = receiver.receive(500);
    assertTrue(message instanceof ActiveMQBlobMessage);
    assertTrue(Files.exists(file));
    assertEquals(content, StreamUtils.copyToString(((ActiveMQBlobMessage) message).getInputStream(), StandardCharsets.UTF_8));

    // The final deletion happens asynchronously and might be a bit delayed
    runWithDelay(10, () -> assertFalse(Files.exists(file)));
  }

  @Test
  public void testActiveMqMultiple() throws Exception {
    Queue destination = session.createQueue("test");
    String content = "Test content";
    Path file = testPath.resolve(UUID.randomUUID().toString());
    Files.write(file, content.getBytes());
    MessageProducer producer = session.createProducer(destination);
    QueueReceiver receiver = session.createReceiver(destination);

    for (int i = 1; i <=3; i++) {
      producer.send(registry.createBlobMessage(session, file, 1));
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
  public void testTtl() throws Exception {
    Queue destination = session.createQueue("test");

    Path file = testPath.resolve(UUID.randomUUID().toString());
    Files.write(file, "Test content".getBytes());
    session.createProducer(destination).send(registry.createBlobMessage(session, file, 1));
    session.createProducer(destination).send(registry.createBlobMessage(session, file, 1));

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

  private void runWithDelay(long delay, Runnable task) throws InterruptedException {
    Thread.sleep(delay);
    task.run();
  }

  public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
    @Override
    public void initialize(ConfigurableApplicationContext applicationContext) {
      Map<String, Object> props = new HashMap<>();
      props.put("server.port", SocketUtils.findAvailableTcpPort());
      props.put("jmsPort", SocketUtils.findAvailableTcpPort());
      props.put("uuid", UUID.randomUUID().toString());
      MapPropertySource propertySource = new MapPropertySource("agent", props);
      applicationContext.getEnvironment().getPropertySources().addFirst(propertySource);

      BrokerService broker = new BrokerService();
      try {
        broker.setUseShutdownHook(false);
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.addConnector("nio://localhost:" + props.get("jmsPort"));
        broker.start();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}