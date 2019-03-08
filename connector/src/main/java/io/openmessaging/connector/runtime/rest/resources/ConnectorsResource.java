package io.openmessaging.connector.runtime.rest.resources;

import io.openmessaging.connector.runtime.Processor;
import io.openmessaging.connector.runtime.rest.entities.*;
import io.openmessaging.connector.runtime.rest.error.ConnectException;
import io.openmessaging.connector.runtime.utils.FutureCallBack;

import javax.ws.rs.*;
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
    public ConnectorInfo createConnector(CreateConnectorRequest request) {
        String name = request.getName().trim();
        FutureCallBack<ConnectorInfo> cb = new FutureCallBack<>();
        processor.putConnectorConfig(name, request.getConfig(), cb);
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
        FutureCallBack<Void> callBack = new FutureCallBack<>();
        processor.restartConnector(connector, callBack);
        waitCallBackComplete(callBack);
        return Response.accepted().build();
    }

    @DELETE
    @Path("/{connector}")
    public ConnectorInfo deleteConnector(final @PathParam("connector") String connector) {
        FutureCallBack<ConnectorInfo> cb = new FutureCallBack<>();
        processor.deleteConnectorConfig(connector, cb);
        return waitCallBackComplete(cb);
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
        FutureCallBack<Void> callBack = new FutureCallBack<>();
        processor.restartTask(taskId, callBack);
        waitCallBackComplete(callBack);
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
