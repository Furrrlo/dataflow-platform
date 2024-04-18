package it.polimi.ds.dataflow.coordinator.js;

import it.polimi.ds.dataflow.utils.SuppressFBWarnings;

import java.io.IOException;
import java.util.Objects;

@SuppressWarnings({
        "serial", // Don't care about this being serializable
        "RedundantSuppression" // Javac complains about serial, IntelliJ about the suppression
})
@SuppressFBWarnings("SE_BAD_FIELD") // Don't care about this being serializable
final class WrappedIOException extends RuntimeException {

    public WrappedIOException(IOException cause) {
        super(cause);
    }

    @Override
    @SuppressWarnings("PMD.AvoidSynchronizedAtMethodLevel") // Overriding a synchronized method
    public synchronized IOException getCause() {
        return (IOException) Objects.requireNonNull(super.getCause());
    }
}
