package it.polimi.ds.dataflow.coordinator.src;

import it.polimi.ds.dataflow.src.Src;
import org.jetbrains.annotations.Unmodifiable;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public sealed interface CoordinatorSrc extends Src permits PartitionedCoordinatorSrc, NonPartitionedCoordinatorSrc {

    enum Kind {
        LINES("lines",
                Arg.required("file", String.class),
                Arg.required("partitions", Integer.class)),
        CSV("csv",
                Arg.required("file", String.class),
                Arg.required("partitions", Integer.class),
                Arg.optional("delimiter", String.class)),
        DFS("dfs",
                Arg.required("file", String.class)),
        REQUIRE("requireInput" /* Its args are special in the parser */);

        public static final @Unmodifiable List<Kind> VALUES = List.of(values());

        private final String methodIdentifier;
        private final int minArgs;
        private final @Unmodifiable Map<String, Arg> args;

        Kind(String methodIdentifier, Arg... args) {
            this.methodIdentifier = methodIdentifier;
            this.minArgs = (int) Arrays.stream(args).filter(Arg::required).count();
            this.args = Arrays.stream(args).collect(Collectors.toMap(Arg::name, Function.identity()));
        }

        public String getMethodIdentifier() {
            return methodIdentifier;
        }

        public int getMinArgs() {
            return minArgs;
        }

        public @Unmodifiable Map<String, Arg> getArgs() {
            return args;
        }

        public record Arg(String name, Class<?> type, boolean required) {

            static Arg required(String name, Class<?> type) {
                return new Arg(name, type, true);
            }

            static Arg optional(String name, Class<?> type) {
                return new Arg(name, type, false);
            }
        }
    }
}
