package it.polimi.ds.dataflow.socket.packets;

public sealed interface JobResultPacket
        extends C2SAckPacket
        permits JobSuccessPacket, JobFailurePacket {
}
