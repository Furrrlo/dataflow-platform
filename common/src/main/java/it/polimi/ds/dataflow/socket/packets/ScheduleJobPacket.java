package it.polimi.ds.dataflow.socket.packets;

import it.polimi.ds.dataflow.js.Op;

import java.util.List;

public record ScheduleJobPacket(
        int jobId,
        List<Op> ops,
        NetDfsFileInfo dfsSrcFile,
        int partition,
        String dfsDstFileName
) implements CoordinatorRequestPacket {

    @Override
    public String toString() {
        return "ScheduleJobPacket{" +
                "jobId=" + jobId +
                ", dfsSrcFile='" + dfsSrcFile + '\'' +
                ", partition=" + partition +
                ", dfsDstFileName='" + dfsDstFileName + '\'' +
                '}';
    }
}
