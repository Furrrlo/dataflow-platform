package it.polimi.ds.map_reduce.js;

import org.jetbrains.annotations.Unmodifiable;

import java.util.List;

public enum OpKind {
    FLAT_MAP("flatMap"),
    MAP("map"),
    FILTER("filter"),
    CHANGE_KEY("changeKey"),
    REDUCE("reduce", true);

    public static final @Unmodifiable List<OpKind> VALUES = List.of(values());

    private final String name;
    private final boolean isTerminal;

    OpKind(String name, boolean isTerminal) {
        this.name = name;
        this.isTerminal = isTerminal;
    }

    OpKind(String name) {
        this(name, false);
    }

    public String getName() {
        return name;
    }

    public boolean isTerminal() {
        return isTerminal;
    }

    @Override
    public String toString() {
        return "OpKind{" +
                "name='" + name + '\'' +
                ", isTerminal=" + isTerminal +
                "}";
    }
}
