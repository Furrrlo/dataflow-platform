package it.polimi.ds.dataflow.coordinator.properties;

import it.polimi.ds.dataflow.PropertiesHandler;

public interface CoordinatorPropertiesHandler extends PropertiesHandler {
    int getListeningPort();
}
