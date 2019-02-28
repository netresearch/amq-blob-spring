package org.netresearch.amqblobspring;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import(value = {BlobController.class, BlobRegistry.class})
public class BlobAutoConfiguration {
}
