package it.polimi.ds.dataflow.js;

import org.jetbrains.annotations.Unmodifiable;

import java.util.List;

public enum OpKind {
    FLAT_MAP("flatMap", true, false),
    MAP("map"),
    FILTER("filter"),
    CHANGE_KEY("changeKey", true, false),
    REDUCE("reduce");

    public static final @Unmodifiable List<OpKind> VALUES = List.of(values());

    private final String name;
    private final boolean shuffles;
    private final boolean isTerminal;

    OpKind(String name, boolean shuffles, boolean isTerminal) {
        this.name = name;
        this.shuffles = shuffles;
        this.isTerminal = isTerminal;
    }

    OpKind(String name) {
        this(name, false, false);
    }

    public String getName() {
        return name;
    }

    public boolean isTerminal() {
        return isTerminal;
    }

    public boolean isShuffles() {
        return shuffles;
    }

    public boolean isRequiresShuffling() {
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
