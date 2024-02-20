package it.polimi.ds.map_reduce.js;

import it.polimi.ds.map_reduce.src.Src;

import java.util.List;

public record CompiledProgram(Src src, int partitions, List<CompiledOp> ops) {

    public CompiledProgram(Src src, int partitions, List<CompiledOp> ops) {
        this.src = src;
        this.partitions = partitions;
        this.ops = List.copyOf(ops);
    }
}
