package org.netresearch.amqblobspring;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.util.StreamUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

@RestController
public class BlobController {
  @Autowired
  private BlobRegistry registry;

  @GetMapping("/blob/{id}")
  public void getFile(HttpServletResponse response, @PathVariable String id) throws IOException {
    final BlobEntry entry = registry.getEntry(id);

    if (entry == null) {
      response.sendError(HttpStatus.FORBIDDEN.value(), "Forbidden");
      return;
    }

    long contentLength = entry.getContentLength();
    if (contentLength > -1) {
      response.setContentLengthLong(contentLength);
    }
    response.setContentType(MediaType.APPLICATION_OCTET_STREAM_VALUE);

    try (InputStream in = entry.getInputStream(); OutputStream out = response.getOutputStream()) {
      StreamUtils.copy(in, out);
    }
  }
}
