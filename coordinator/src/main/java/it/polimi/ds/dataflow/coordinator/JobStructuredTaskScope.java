package it.polimi.ds.dataflow.coordinator;

import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Function;
import java.util.stream.IntStream;

public class JobStructuredTaskScope<T> extends StructuredTaskScope<JobStructuredTaskScope.PartitionResult<T>> {

    private final AtomicReferenceArray<@Nullable PartitionResult<T>> results;
    private final AtomicReference<@Nullable Throwable> firstException = new AtomicReference<>();

    private final int partitions;
    private final AtomicInteger foundResults = new AtomicInteger();

    public JobStructuredTaskScope(String name, ThreadFactory factory, int partitions) {
        super(name, factory);
        this.results = new AtomicReferenceArray<>(this.partitions = partitions);
    }

    public JobStructuredTaskScope(int partitions) {
        this.results = new AtomicReferenceArray<>(this.partitions = partitions);
    }

    @Override
    protected void handleComplete(Subtask<? extends PartitionResult<T>> subtask) {
        if (subtask.state() == Subtask.State.FAILED
                && firstException.compareAndSet(null, subtask.exception())) {
            super.shutdown();
            return;
        }

        if (subtask.state() == Subtask.State.SUCCESS) {
            PartitionResult<T> result = subtask.get();
            if (results.compareAndSet(result.partition(), null, result) &&
                    foundResults.incrementAndGet() >= partitions) {
                super.shutdown();
            }
        }
    }

    @Override
    public JobStructuredTaskScope<T> join() throws InterruptedException {
        super.join();
        return this;
    }

    @Override
    public JobStructuredTaskScope<T> joinUntil(Instant deadline) throws InterruptedException, TimeoutException {
        super.joinUntil(deadline);
        return this;
    }

    public List<PartitionResult<T>> result() throws ExecutionException {
        return result(ExecutionException::new);
    }

    public <X extends Throwable> List<PartitionResult<T>> result(Function<Throwable, ? extends X> esf) throws X {
        Objects.requireNonNull(esf);
        ensureOwnerAndJoined();

        if (foundResults.get() >= partitions)
            return IntStream.range(0, results.length())
                    .mapToObj(i -> Objects.requireNonNull(results.get(i), "Task result cannot be null"))
                    .toList();

        Throwable exception = firstException.get();
        if (exception != null) {
            X ex = esf.apply(exception);
            Objects.requireNonNull(ex, "esf returned null");
            throw ex;
        }

        throw new IllegalStateException("No completed subtasks");
    }

    public record PartitionResult<T>(int partition, T result) {
    }
}
