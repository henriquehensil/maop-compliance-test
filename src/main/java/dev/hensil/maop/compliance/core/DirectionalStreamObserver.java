package dev.hensil.maop.compliance.core;

import dev.hensil.maop.compliance.model.operation.Operation;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

final class DirectionalStreamObserver {

    // Objects

    private final @NotNull DirectionalStream stream;
    private final @NotNull LinkedBlockingQueue<Operation> globalOperations = new LinkedBlockingQueue<>(30);

    // Constructor

    DirectionalStreamObserver(@NotNull DirectionalStream stream) {
        this.stream = stream;
    }

    // Modules

    void fire(@NotNull Operation operation) {
        verifications(operation);

        try {
            boolean offered = this.globalOperations.offer(operation, 1, TimeUnit.SECONDS);
            if (!offered) {
                throw new IllegalStateException("The queue of operations is full");
            }
        } catch (InterruptedException ignore) {

        }
    }

    public @NotNull Operation await(int timeout, @NotNull TimeUnit timeUnit) throws TimeoutException, InterruptedException {
        @Nullable Operation operation = this.globalOperations.poll(timeout, timeUnit);
        if (operation == null) {
            throw new TimeoutException();
        }

        return operation;
    }

    private void verifications(@Nullable Operation operation) {
        if (!stream.getConnection().isConnected()) {
            throw new IllegalStateException("Connection lost");
        }

        if (operation != null && this.globalOperations.contains(operation)) {
            throw new IllegalArgumentException("Reference already fired: " + operation);
        }
    }
}