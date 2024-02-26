package it.polimi.ds.dataflow.socket.packets;

public record CreateFilePartitionFailurePacket(Exception exception) implements CreateFilePartitionResultPacket {
}
