package it.polimi.ds.map_reduce.js;

import it.polimi.ds.map_reduce.Tuple2;
import it.polimi.ds.map_reduce.src.Src;
import it.polimi.ds.map_reduce.utils.SuppressFBWarnings;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public record CompiledProgram(Src src, List<CompiledOp> ops) {

    @SuppressFBWarnings(
            value = "OCP_OVERLY_CONCRETE_PARAMETER",
            justification =  "Can't make it more general 'cause it's the canonical ctor")
    public CompiledProgram(Src src, List<CompiledOp> ops) {
        this.src = src;
        this.ops = List.copyOf(ops);
    }

    public List<Tuple2> execute() throws IOException {
        try(Stream<Tuple2> stream = src.loadAll()) {
            return execute(ops, stream).toList();
        }
    }

    public static Stream<Tuple2> execute(List<CompiledOp> ops, Stream<Tuple2> data) {
        for (CompiledOp op : ops) {
            data = switch (op) {
                case FilterCompiledOp filter -> data.filter(filter);
                case MapCompiledOp map -> data.map(map);
                case ChangeKeyCompiledOp map -> data.map(map);
                case FlatMapCompiledOp flatMap -> data
                        .flatMap(t -> flatMap.apply(t)
                                .entrySet()
                                .stream()
                                .map(e -> new Tuple2(e.getKey(), e.getValue())));
                case ReduceCompiledOp reduce -> //noinspection DataFlowIssue
                        data.collect(Collectors.groupingBy(Tuple2::key, Collectors.toList()))
                                .entrySet()
                                .stream()
                                .map(e -> reduce.apply(e.getKey(), e.getValue().stream()
                                        .map(Tuple2::value)
                                        .collect(Collectors.toList())));
            };
        }
        return data;
    }
}
