package it.polimi.ds.dataflow.socket.packets;

import org.jspecify.annotations.Nullable;

import java.util.List;

public record CleanupIntermediateFilesPacket(
        int jobId,
        @Nullable NetDfsFileInfo srcFile,
        List<NetDfsFileInfo> exclude
) implements CoordinatorRequestPacket {
}
