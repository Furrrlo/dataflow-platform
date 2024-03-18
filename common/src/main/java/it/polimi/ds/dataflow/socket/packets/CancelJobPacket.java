package it.polimi.ds.dataflow.socket.packets;

public record CancelJobPacket(int jobId, int partition) implements CoordinatorRequestPacket {
}
