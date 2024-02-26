package it.polimi.ds.dataflow.worker.socket;

import it.polimi.ds.dataflow.socket.SocketManager;
import it.polimi.ds.dataflow.socket.packets.C2SAckPacket;
import it.polimi.ds.dataflow.socket.packets.C2SPacket;
import it.polimi.ds.dataflow.socket.packets.S2CAckPacket;
import it.polimi.ds.dataflow.socket.packets.S2CPacket;

public interface WorkerSocketManager extends SocketManager<S2CPacket, S2CAckPacket, C2SAckPacket, C2SPacket> {
}
