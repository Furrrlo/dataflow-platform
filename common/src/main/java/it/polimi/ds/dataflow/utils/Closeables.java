package it.polimi.ds.dataflow.utils;

import org.jspecify.annotations.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Function;
import java.util.stream.Stream;

@SuppressWarnings({
        "unused",
        "PMD.SignatureDeclareThrowsException" // AutoCloseables throw Exceptions
})
public final class Closeables {

    private Closeables() {
    }

    public static Closeable compose(Collection<? extends Closeable> closeables) {
        return () -> closeAll(closeables.stream());
    }

    public static Closeable compose(Collection<? extends Closeable> closeables,
                                    Function<? super @Nullable IOException, ? extends IOException> esf) {
        return () -> closeAll(closeables.stream(), esf);
    }

    public static Closeable compose(Stream<? extends Closeable> closeables) {
        return () -> closeAll(closeables);
    }

    public static Closeable compose(Stream<? extends Closeable> closeables,
                                    Function<? super @Nullable IOException, ? extends IOException> esf) {
        return () -> closeAll(closeables, esf);
    }

    public static void closeAll(Collection<? extends Closeable> closeables) throws IOException {
        closeAll(closeables.stream());
    }

    public static void closeAll(Collection<? extends Closeable> closeables,
                                Function<? super @Nullable IOException, ? extends IOException> esf)
            throws IOException {
        doCloseAll(closeables.stream(), esf);
    }

    public static void closeAll(Stream<? extends Closeable> closeables) throws IOException {
        doCloseAll(closeables, (@Nullable IOException e) -> new IOException(e));
    }

    public static void closeAll(Stream<? extends Closeable> closeables,
                                Function<? super @Nullable IOException, ? extends IOException> esf)
            throws IOException {
        doCloseAll(closeables, esf);
    }

    public static final class Auto {

        private Auto() {
        }

        public static AutoCloseable compose(Collection<? extends AutoCloseable> closeables) {
            return () -> closeAll(closeables.stream());
        }

        public static AutoCloseable compose(Collection<? extends AutoCloseable> closeables,
                                            Function<? super @Nullable Exception, ? extends Exception> esf) {
            return () -> closeAll(closeables.stream(), esf);
        }

        public static AutoCloseable compose(Stream<? extends AutoCloseable> closeables) {
            return () -> closeAll(closeables);
        }

        public static AutoCloseable compose(Stream<? extends AutoCloseable> closeables,
                                            Function<? super @Nullable Exception, ? extends Exception> esf) {
            return () -> closeAll(closeables, esf);
        }

        public static void closeAll(Collection<? extends AutoCloseable> closeables) throws Exception {
            closeAll(closeables.stream());
        }

        public static void closeAll(Collection<? extends AutoCloseable> closeables,
                                    Function<? super @Nullable Exception, ? extends Exception> esf)
                throws Exception {
            doCloseAll(closeables.stream(), esf);
        }

        public static void closeAll(Stream<? extends AutoCloseable> closeables) throws Exception {
            doCloseAll(closeables, (Exception e) -> new Exception(e));
        }

        public static void closeAll(Stream<? extends AutoCloseable> closeables,
                                    Function<? super @Nullable Exception, ? extends Exception> esf)
                throws Exception {
            doCloseAll(closeables, esf);
        }
    }

    @SuppressWarnings("unchecked")
    private static <E extends Exception> void doCloseAll(Stream<? extends AutoCloseable> closeables,
                                                         Function<? super @Nullable E, ? extends E> esf)
            throws E {

        var exs = new ArrayList<E>();
        closeables.forEach(c -> {
            try {
                c.close();
            } catch (Exception t) {
                exs.add((E) t);
            }
        });

        switch (exs.size()) {
            case 0 -> {}
            case 1 -> throw esf.apply(exs.getFirst());
            default -> {
                E ex = esf.apply(null);
                exs.forEach(ex::addSuppressed);
                throw ex;
            }
        }
    }
}
