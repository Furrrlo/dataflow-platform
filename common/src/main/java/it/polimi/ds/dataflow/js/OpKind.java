package it.polimi.ds.dataflow.js;

import org.jetbrains.annotations.Unmodifiable;

import java.util.List;

public enum OpKind {
    FLAT_MAP("flatMap", OpKind.SHUFFLES),
    MAP("map"),
    FILTER("filter"),
    CHANGE_KEY("changeKey", OpKind.SHUFFLES),
    REDUCE("reduce");

    public static final @Unmodifiable List<OpKind> VALUES = List.of(values());

    private static final int SHUFFLES = 0x1;
    private static final int REQUIRES_SHUFFLE = 0x2;
    private static final int TERMINAL = 0x4;

    private final String name;
    private final int flags;

    OpKind(String name, int flags) {
        this.name = name;
        this.flags = flags;
    }

    OpKind(String name) {
        this(name, 0);
    }

    public String getName() {
        return name;
    }

    public boolean isTerminal() {
        return (flags & TERMINAL) != 0;
    }

    public boolean isShuffles() {
        return (flags & SHUFFLES) != 0;
    }

    public boolean isRequiresShuffling() {
        return (flags & REQUIRES_SHUFFLE) != 0;
    }

    @Override
    public String toString() {
        return "OpKind{" +
                "name='" + name + '\'' +
                ", isTerminal=" + isTerminal() +
                ", isShuffles=" + isShuffles() +
                ", isRequiresShuffling=" + isRequiresShuffling() +
                "}";
    }
}
