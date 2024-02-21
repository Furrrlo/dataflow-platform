package it.polimi.ds.map_reduce.coordinator;

import it.polimi.ds.map_reduce.socket.SocketManager;
import it.polimi.ds.map_reduce.socket.packets.C2SAckPacket;
import it.polimi.ds.map_reduce.socket.packets.C2SPacket;
import it.polimi.ds.map_reduce.socket.packets.S2CAckPacket;
import it.polimi.ds.map_reduce.socket.packets.S2CPacket;

public interface CoordinatorSocketManager extends SocketManager<C2SPacket, C2SAckPacket, S2CAckPacket, S2CPacket> {
}
