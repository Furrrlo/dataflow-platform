package it.polimi.ds.dataflow.socket.packets;

import java.util.UUID;

public record HelloPacket(UUID clientUUID) implements C2SPacket{
}
