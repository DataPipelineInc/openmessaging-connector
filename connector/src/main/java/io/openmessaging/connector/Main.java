package io.openmessaging.connector;

import io.openmessaging.KeyValue;
import io.openmessaging.OMS;
import io.openmessaging.connector.runtime.Processor;
import io.openmessaging.connector.runtime.StandaloneProcessor;
import io.openmessaging.connector.runtime.Worker;
import io.openmessaging.connector.runtime.WorkerConfig;
import io.openmessaging.connector.runtime.isolation.Plugins;
import io.openmessaging.connector.runtime.rest.RestServer;
import io.openmessaging.connector.runtime.rest.storage.ConfigStorageService;
import io.openmessaging.connector.runtime.rest.storage.PositionStorageService;
import io.openmessaging.connector.runtime.rest.storage.StatusStorageService;
import io.openmessaging.connector.runtime.storage.standalone.MemoryConfigStorage;
import io.openmessaging.connector.runtime.storage.standalone.MemoryPositionStorage;
import io.openmessaging.connector.runtime.storage.standalone.MemoryStatusStorage;

public class Main {
  public static void main(String[] args) {
    KeyValue restConfig = OMS.newKeyValue();
    restConfig.put("hostname", "localhost");
    restConfig.put("port", 2222);
    KeyValue messagingSystemConfig = OMS.newKeyValue();
    messagingSystemConfig.put("accessPoint", "oms:rocketmq://wh:9876/default:default");
    WorkerConfig workerConfig =
        new WorkerConfig(restConfig, messagingSystemConfig, OMS.newKeyValue());
    ConfigStorageService configStorageService = new MemoryConfigStorage();
    StatusStorageService statusStorageService = new MemoryStatusStorage();
    PositionStorageService positionStorageService = new MemoryPositionStorage();
    Worker worker = new Worker(workerConfig, positionStorageService, new Plugins());
    Processor processor =
        new StandaloneProcessor(configStorageService, statusStorageService, worker);
    RestServer restServer = new RestServer(workerConfig);
    restServer.startServer(processor);
  }
}
