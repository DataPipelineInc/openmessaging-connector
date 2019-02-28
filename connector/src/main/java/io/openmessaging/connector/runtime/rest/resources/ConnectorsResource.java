package io.openmessaging.connector.runtime.rest.resources;

import io.openmessaging.connector.runtime.Processor;
import io.openmessaging.connector.runtime.rest.entities.ConnectorInfo;
import io.openmessaging.connector.runtime.rest.entities.ConnectorStateInfo;
import io.openmessaging.connector.runtime.rest.entities.ConnectorTaskId;
import io.openmessaging.connector.runtime.rest.entities.TaskInfo;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;

@Path("/connector")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class ConnectorsResource {
  private Processor processor;

  public ConnectorsResource(Processor processor) {
    this.processor = processor;
  }

  @POST
  @Path("/")
  public Response createConnector(Map<String, String> connectorConfig) {
    String name = connectorConfig.get("name").trim();
    ConnectorInfo connectorInfo = processor.putConnectorConfig(name, connectorConfig);
    return Response.accepted().entity(connectorInfo).build();
  }

  @GET
  @Path("/{connector}")
  public ConnectorInfo getConnector(final @PathParam("connector") String connector) {
    return processor.connectorConfig(connector);
  }

  @GET
  @Path("/{connector}/config")
  public Map<String, String> getConnectorConfig(@PathParam("connector") String connector) {
    return processor.connectorConfig(connector).getConnectorConfig();
  }

  @GET
  @Path("/{connector}/status")
  public ConnectorStateInfo getConnectorStatus(final @PathParam("connector") String connector) {
    return processor.connectorStatus(connector);
  }

  @PUT
  @Path("/{connector}/pause")
  public Response pauseConnector(final @PathParam("connector") String connector) {
    processor.pauseConnector(connector);
    return Response.accepted().build();
  }

  @PUT
  @Path("/{connector}/resume")
  public Response resumeConnector(final @PathParam("connector") String connector) {
    processor.resumeConnector(connector);
    return Response.accepted().build();
  }

  @POST
  @Path("/{connector}/restart")
  public Response restartConnector(final @PathParam("connector") String connector) {
    processor.restartConnector(connector);
    return Response.accepted().build();
  }

  @DELETE
  @Path("/{connector}")
  public Response deleteConnector(final @PathParam("connector") String connector) {
    processor.deleteConnectorConfig(connector);
    return Response.accepted().build();
  }

  @GET
  @Path("/{connector}/tasks")
  public List<TaskInfo> getTaskConfigs(@PathParam("connector") String connector) {
    return processor.taskConfigs(connector);
  }

  @POST
  @Path("/{connector}/tasks")
  public List<TaskInfo> putTaskConfigs(
      @PathParam("connector") String connector, List<Map<String, String>> taskConfigs) {
    return processor.putTaskConfig(connector, taskConfigs);
  }

  @GET
  @Path("/{connector}/tasks/{task}/status")
  public ConnectorStateInfo.TaskState getTaskStatus(
      @PathParam("connector") String connector, @PathParam("task") Integer task) {
    return processor.taskStatus(new ConnectorTaskId(connector, task));
  }

  @POST
  @Path("/{connector}/tasks/{task}/restart")
  public Response restartTask(
      @PathParam("connector") String connector, @PathParam("task") Integer task) {
    ConnectorTaskId taskId = new ConnectorTaskId(connector, task);
    processor.restartTask(taskId);
    return Response.accepted().build();
  }
}
