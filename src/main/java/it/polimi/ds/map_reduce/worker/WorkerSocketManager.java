package it.polimi.ds.map_reduce.worker;

import it.polimi.ds.map_reduce.socket.SocketManager;
import it.polimi.ds.map_reduce.socket.packets.C2SAckPacket;
import it.polimi.ds.map_reduce.socket.packets.C2SPacket;
import it.polimi.ds.map_reduce.socket.packets.S2CAckPacket;
import it.polimi.ds.map_reduce.socket.packets.S2CPacket;

public interface WorkerSocketManager extends SocketManager<S2CPacket, S2CAckPacket, C2SAckPacket, C2SPacket> {
}
