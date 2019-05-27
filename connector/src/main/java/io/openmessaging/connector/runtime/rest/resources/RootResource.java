package io.openmessaging.connector.runtime.rest.resources;

import io.openmessaging.connector.runtime.Processor;
import io.openmessaging.connector.runtime.rest.entities.MessageSystem;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class RootResource {
  private Processor processor;

  public RootResource(Processor processor) {
    this.processor = processor;
  }

  @GET
  public MessageSystem getInfo() {
    return this.processor.getInfo();
  }
}
