package org.netresearch.amqblobspring;

import org.apache.activemq.broker.BrokerService;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.MapPropertySource;
import org.springframework.util.SocketUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@SpringBootConfiguration
@EnableAutoConfiguration
@SuppressWarnings({"EmptyClass", "WeakerAccess"})
public class BlobTestConfiguration {
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
