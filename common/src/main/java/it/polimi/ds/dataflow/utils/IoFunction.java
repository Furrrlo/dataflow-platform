package it.polimi.ds.dataflow.utils;

import java.io.IOException;

@FunctionalInterface
public interface IoFunction<T, R> {

    R apply(T t) throws IOException;
}
