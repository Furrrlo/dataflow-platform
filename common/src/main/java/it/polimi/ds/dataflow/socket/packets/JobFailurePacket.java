package it.polimi.ds.dataflow.socket.packets;

public record JobFailurePacket(Exception ex) implements JobResultPacket {
}
