package io.openmessaging.connector.runtime.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import io.openmessaging.connector.runtime.Processor;
import io.openmessaging.connector.runtime.WorkerConfig;
import io.openmessaging.connector.runtime.rest.error.ConnectException;
import io.openmessaging.connector.runtime.rest.resources.ConnectorsResource;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.Slf4jRequestLog;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class RestServer {
    private static final Logger log = LoggerFactory.getLogger(RestServer.class);
    private static final long GRACEFUL_SHUTDOWN_TIMEOUT_MS = 60 * 1000;
    private Server server;
    private WorkerConfig workerConfig;

    public RestServer(WorkerConfig workerConfig) {
        this.server = new Server();
        this.workerConfig = workerConfig;
        this.setConnector();
    }

    private void setConnector() {
        getListeners(workerConfig)
                .forEach(
                        (hostname, port) -> {
                            ServerConnector connector = new ServerConnector(server);
                            connector.setName(String.format("%s:%s", hostname, port));
                            connector.setHost(hostname);
                            connector.setPort(port);
                            this.server.addConnector(connector);
                        });
    }

    private Map<String, Integer> getListeners(WorkerConfig workerConfig) {
        Map<String, Integer> listeners = new HashMap<>();
        listeners.put(
                workerConfig.getRestConfig().getString(WorkerConfig.REST_HOSTNAME),
                workerConfig.getRestConfig().getInt(WorkerConfig.REST_PORT));
        return listeners;
    }

    public void startServer(Processor processor) {
        log.info("Starting REST Server");

        ResourceConfig resourceConfig = new ResourceConfig();

        JacksonJsonProvider provider = new JacksonJsonProvider();
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        provider.setMapper(objectMapper);

        resourceConfig.register(provider);
        resourceConfig.register(new ConnectorsResource(processor));

        ServletContainer servletContainer = new ServletContainer(resourceConfig);
        ServletHolder servletHolder = new ServletHolder(servletContainer);

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        context.addServlet(servletHolder, "/*");

        RequestLogHandler requestLogHandler = new RequestLogHandler();
        Slf4jRequestLog requestLog = new Slf4jRequestLog();
        requestLog.setLoggerName(RestServer.class.getCanonicalName());
        requestLog.setLogLatency(true);
        requestLogHandler.setRequestLog(requestLog);

        HandlerCollection handlers = new HandlerCollection();
        handlers.setHandlers(new Handler[]{context, new DefaultHandler(), requestLogHandler});

        StatisticsHandler statsHandler = new StatisticsHandler();
        statsHandler.setHandler(handlers);
        server.setHandler(statsHandler);
        server.setStopTimeout(GRACEFUL_SHUTDOWN_TIMEOUT_MS);
        server.setStopAtShutdown(true);
        try {
            server.start();
        } catch (Exception e) {
            throw new ConnectException("Unable to start REST server", e);
        }
    }

    public void stopServer() {
        log.info("Stopping REST Server");
        try {
            this.server.stop();
            this.server.join();
        } catch (Exception e) {
            throw new ConnectException("Unable to stop REST server", e);
        } finally {
            this.server.destroy();
        }
    }
}
