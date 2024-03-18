package it.polimi.ds.dataflow.utils;

import org.jspecify.annotations.Nullable;

@SuppressWarnings({
        "serial", // Don't care about this being serializable
        "RedundantSuppression" // Javac complains about serial, IntelliJ about the suppression
})
public class UncheckedInterruptedException extends RuntimeException {

    public UncheckedInterruptedException() {
    }

    public UncheckedInterruptedException(@Nullable Throwable cause) {
        super(cause);
    }
}
