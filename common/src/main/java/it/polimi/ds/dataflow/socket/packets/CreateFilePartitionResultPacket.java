package it.polimi.ds.dataflow.socket.packets;

public sealed interface CreateFilePartitionResultPacket
        extends C2SAckPacket
        permits CreateFilePartitionSuccessPacket, CreateFilePartitionFailurePacket {
}
