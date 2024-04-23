package it.polimi.ds.dataflow.coordinator.properties;

import it.polimi.ds.dataflow.CommonPropertiesHandler;
import it.polimi.ds.dataflow.src.WorkDirFileLoader;

import java.io.IOException;


public class CoordinatorPropertiesHandlerImpl extends CommonPropertiesHandler implements CoordinatorPropertiesHandler {

    private static final String DEFAULT_PROPERTIES_FILE_NAME = "coordinator.properties";

    public CoordinatorPropertiesHandlerImpl(WorkDirFileLoader fileLoader) throws IOException {
        super(fileLoader.resolvePath(DEFAULT_PROPERTIES_FILE_NAME), "jasypt");
    }

    @Override
    public int getListeningPort() {
        return Integer.parseInt(getProperty("LISTENING_PORT"));
    }
}
