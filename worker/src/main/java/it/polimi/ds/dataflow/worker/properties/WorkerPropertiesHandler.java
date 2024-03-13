package it.polimi.ds.dataflow.worker.properties;

import it.polimi.ds.dataflow.PropertiesHandler;

import java.util.UUID;

public interface WorkerPropertiesHandler extends PropertiesHandler {
    UUID getUuid();
    String getDfsCoordinatorName();
    String getCoordinatorIp();
    int getCoordinatorPort();
}
