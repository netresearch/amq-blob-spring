package org.netresearch.amqblobspring;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

@RestController
public class BlobController {
  @Autowired
  private BlobRegistry registry;

  @GetMapping("/blob/{id}")
  public ResponseEntity<?> getFile(@PathVariable String id) {
    BlobEntry blobEntry = registry.getFileEntry(id);
    InputStream inputStream = null;
    if (blobEntry != null) {
      try {
        inputStream = new DeleteOnCloseFileInputStream(blobEntry);
      } catch (FileNotFoundException e) {
        blobEntry.remove();
      }
    }

    if (inputStream == null) {
      return ResponseEntity.status(HttpStatus.FORBIDDEN).body("Forbidden");
    }

    return ResponseEntity.ok()
        .contentType(MediaType.APPLICATION_OCTET_STREAM)
        .contentLength(blobEntry.getPath().toFile().length())
        .body(new InputStreamResource(inputStream));
  }

  private static class DeleteOnCloseFileInputStream extends FileInputStream {
    private final BlobEntry blobEntry;

    DeleteOnCloseFileInputStream(BlobEntry blobEntry) throws FileNotFoundException {
      super(blobEntry.getPath().toFile());
      this.blobEntry = blobEntry;
      blobEntry.markRunning();
    }

    @Override
    public void close() throws IOException {
      try {
        super.close();
      } finally {
        blobEntry.markDone();
      }
    }
  }
}
