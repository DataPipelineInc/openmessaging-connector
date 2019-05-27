package io.openmessaging.connector;

import io.openmessaging.connector.runtime.Processor;
import io.openmessaging.connector.runtime.StandaloneProcessor;
import io.openmessaging.connector.runtime.Worker;
import io.openmessaging.connector.runtime.WorkerConfig;
import io.openmessaging.connector.runtime.isolation.Plugins;
import io.openmessaging.connector.runtime.rest.RestServer;
import io.openmessaging.connector.runtime.rest.entities.MessageSystem;
import io.openmessaging.connector.runtime.rest.error.ConnectException;
import io.openmessaging.connector.runtime.rest.storage.ConfigStorageService;
import io.openmessaging.connector.runtime.rest.storage.PositionStorageService;
import io.openmessaging.connector.runtime.rest.storage.StatusStorageService;
import io.openmessaging.connector.runtime.storage.standalone.MemoryConfigStorage;
import io.openmessaging.connector.runtime.storage.standalone.MemoryPositionStorage;
import io.openmessaging.connector.runtime.storage.standalone.MemoryStatusStorage;
import io.openmessaging.connector.runtime.utils.ConvertUtils;
import io.openmessaging.connector.runtime.utils.Utils;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public class Main {
  public static void main(String[] args) {
    if (args.length < 1) {
      throw new ConnectException("error args");
    }
    Map<String, String> workerConfigMap =
        ConvertUtils.propertiesToMap(Utils.getProperties(args[0]));
    WorkerConfig workerConfig = new WorkerConfig(workerConfigMap);
    String accessPointString =
        workerConfig.getWorkerConfig().getString(WorkerConfig.OMS_ACCESSPOINT);
    String unprocessedString = accessPointString.substring("oms:".length());
    String driverType = unprocessedString.substring(0, unprocessedString.indexOf(":"));
    if (StringUtils.isNoneBlank(driverType)
        && driverType.equals(
            workerConfig.getWorkerConfig().getString(WorkerConfig.MESSAGE_SYSTEM_NAME))) {
      ConfigStorageService configStorageService = new MemoryConfigStorage();
      StatusStorageService statusStorageService = new MemoryStatusStorage();
      PositionStorageService positionStorageService = new MemoryPositionStorage();
      Worker worker = new Worker(workerConfig, positionStorageService, new Plugins());
      MessageSystem messageSystem =
          new MessageSystem(
              workerConfig.getWorkerConfig().getString(WorkerConfig.MESSAGE_SYSTEM_NAME),
              workerConfig.getWorkerConfig().getString(WorkerConfig.MESSAGE_SYSTEM_VERSION));
      Processor processor =
          new StandaloneProcessor(
              configStorageService, statusStorageService, worker, messageSystem);
      RestServer restServer = new RestServer(workerConfig);
      processor.start();
      restServer.startServer(processor);
    } else {
      throw new ConnectException(
          "未找到消息中间件驱动 : "
              + workerConfig.getWorkerConfig().getString(WorkerConfig.MESSAGE_SYSTEM_NAME));
    }
  }
}
