package it.polimi.ds.map_reduce.coordinator;

import it.polimi.ds.map_reduce.socket.SocketManagerImpl;
import it.polimi.ds.map_reduce.socket.packets.C2SAckPacket;
import it.polimi.ds.map_reduce.socket.packets.C2SPacket;
import it.polimi.ds.map_reduce.socket.packets.S2CAckPacket;
import it.polimi.ds.map_reduce.socket.packets.S2CPacket;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.ExecutorService;

public class CoordinatorSocketManagerImpl
        extends SocketManagerImpl<C2SPacket, C2SAckPacket, S2CAckPacket, S2CPacket>
        implements CoordinatorSocketManager {

    public CoordinatorSocketManagerImpl(ExecutorService executor, Socket socket) throws IOException {
        super("Coordinator", executor, socket);
    }
}
