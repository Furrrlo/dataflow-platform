package it.polimi.ds.dataflow.dfs;

public record DfsFilePartitionInfo(String fileName, int partition, String dfsNodeName, boolean isLocal) {

    public String partitionFileName() {
        return fileName + "_" + partition; // TODO
    }
}
