package it.polimi.ds.dataflow.socket;

import it.polimi.ds.dataflow.socket.packets.C2SPacket;
import it.polimi.ds.dataflow.socket.packets.S2CPacket;

/** Packet which indicates that the other side is closing */
record ClosePacket() implements S2CPacket, C2SPacket {
}
