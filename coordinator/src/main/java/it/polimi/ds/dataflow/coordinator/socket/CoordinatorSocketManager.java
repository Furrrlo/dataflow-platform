package it.polimi.ds.dataflow.coordinator.socket;

import it.polimi.ds.dataflow.socket.SocketManager;
import it.polimi.ds.dataflow.socket.packets.C2SAckPacket;
import it.polimi.ds.dataflow.socket.packets.C2SPacket;
import it.polimi.ds.dataflow.socket.packets.S2CAckPacket;
import it.polimi.ds.dataflow.socket.packets.S2CPacket;

public interface CoordinatorSocketManager extends SocketManager<C2SPacket, C2SAckPacket, S2CAckPacket, S2CPacket> {
}
