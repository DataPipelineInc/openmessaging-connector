package io.openmessaging.connector.runtime.rest.resources;

import io.openmessaging.connector.runtime.Processor;
import io.openmessaging.connector.runtime.rest.entities.ConnectorInfo;
import io.openmessaging.connector.runtime.rest.entities.ConnectorStateInfo;
import io.openmessaging.connector.runtime.rest.entities.ConnectorTaskId;
import io.openmessaging.connector.runtime.rest.entities.TaskInfo;
import io.openmessaging.connector.runtime.rest.error.ConnectException;
import io.openmessaging.connector.runtime.utils.CallBack;
import io.openmessaging.connector.runtime.utils.FutureCallBack;

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
  public ConnectorInfo createConnector(Map<String, String> connectorConfig) {
    String name = connectorConfig.get("name").trim();
    FutureCallBack<ConnectorInfo> cb = new FutureCallBack<>();
    processor.putConnectorConfig(name, connectorConfig, cb);
    return waitCallBackComplete(cb);
  }

  @GET
  @Path("/{connector}")
  public ConnectorInfo getConnector(final @PathParam("connector") String connector) {
    FutureCallBack<ConnectorInfo> cb = new FutureCallBack<>();
    processor.connectorInfo(connector, cb);
    return waitCallBackComplete(cb);
  }

  @GET
  @Path("/{connector}/config")
  public Map<String, String> getConnectorConfig(@PathParam("connector") String connector) {
    FutureCallBack<Map<String, String>> cb = new FutureCallBack<>();
    processor.connectorConfig(connector, cb);
    return waitCallBackComplete(cb);
  }

  @GET
  @Path("/{connector}/status")
  public ConnectorStateInfo getConnectorStatus(final @PathParam("connector") String connector) {
    FutureCallBack<ConnectorStateInfo> cb = new FutureCallBack<>();
    processor.connectorStatus(connector, cb);
    return waitCallBackComplete(cb);
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
    FutureCallBack<List<TaskInfo>> cb = new FutureCallBack<>();
    processor.taskConfigs(connector, cb);
    return waitCallBackComplete(cb);
  }

  @POST
  @Path("/{connector}/tasks")
  public List<TaskInfo> putTaskConfigs(
      @PathParam("connector") String connector, List<Map<String, String>> taskConfigs) {
    FutureCallBack<List<TaskInfo>> cb = new FutureCallBack<>();
    processor.putTaskConfig(connector, taskConfigs, cb);
    return waitCallBackComplete(cb);
  }

  @GET
  @Path("/{connector}/tasks/{task}/status")
  public ConnectorStateInfo.TaskState getTaskStatus(
      @PathParam("connector") String connector, @PathParam("task") Integer task) {
    FutureCallBack<ConnectorStateInfo.TaskState> cb = new FutureCallBack<>();
    processor.taskStatus(new ConnectorTaskId(connector, task), cb);
    return waitCallBackComplete(cb);
  }

  @POST
  @Path("/{connector}/tasks/{task}/restart")
  public Response restartTask(
      @PathParam("connector") String connector, @PathParam("task") Integer task) {
    ConnectorTaskId taskId = new ConnectorTaskId(connector, task);
    processor.restartTask(taskId);
    return Response.accepted().build();
  }

  private <T> T waitCallBackComplete(FutureCallBack<T> callBack) {
    try {
      return callBack.get();
    } catch (InterruptedException e) {
      throw new ConnectException(e);
    }
  }
}
