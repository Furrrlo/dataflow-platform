package it.polimi.ds.map_reduce.socket;

import it.polimi.ds.map_reduce.socket.packets.C2SAckPacket;
import it.polimi.ds.map_reduce.socket.packets.S2CAckPacket;

/** Acknowledge that {@link it.polimi.ds.map_reduce.socket.ClosePacket} was received and this side is also starting to close */
record CloseAckPacket() implements S2CAckPacket, C2SAckPacket {
}
