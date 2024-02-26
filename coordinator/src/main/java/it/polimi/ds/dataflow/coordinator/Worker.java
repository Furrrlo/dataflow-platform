package it.polimi.ds.dataflow.coordinator;

import it.polimi.ds.dataflow.coordinator.socket.CoordinatorSocketManager;

public record Worker(CoordinatorSocketManager socket, String dfsNodeName) {
}
