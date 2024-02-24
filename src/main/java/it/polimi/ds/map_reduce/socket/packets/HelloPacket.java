package it.polimi.ds.map_reduce.socket.packets;

import java.util.UUID;

public record HelloPacket(UUID clientUUID) implements C2SPacket{
}
