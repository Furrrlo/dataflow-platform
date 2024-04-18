package it.polimi.ds.dataflow.coordinator.js;

import it.polimi.ds.dataflow.utils.SuppressFBWarnings;

import javax.script.ScriptException;
import java.util.Objects;

@SuppressWarnings({
        "serial", // Don't care about this being serializable
        "RedundantSuppression" // Javac complains about serial, IntelliJ about the suppression
})
@SuppressFBWarnings("SE_BAD_FIELD") // Don't care about this being serializable
final class WrappedScriptException extends RuntimeException {

    public WrappedScriptException(ScriptException cause) {
        super(cause);
    }

    @Override
    @SuppressWarnings("PMD.AvoidSynchronizedAtMethodLevel") // Overriding a synchronized method
    public synchronized ScriptException getCause() {
        return (ScriptException) Objects.requireNonNull(super.getCause());
    }
}
