package it.polimi.ds.dataflow.socket.packets;

import java.util.UUID;

public record HelloPacket(UUID uuid, String dfsNodeName) implements C2SPacket{
}
