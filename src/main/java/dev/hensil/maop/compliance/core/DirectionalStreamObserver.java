package dev.hensil.maop.compliance.core;

import dev.hensil.maop.compliance.model.operation.Operation;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

final class DirectionalStreamObserver {

    // Objects

    private final @NotNull DirectionalStream stream;
    private final @NotNull LinkedBlockingQueue<Operation> globalOperations = new LinkedBlockingQueue<>(30);

    private volatile @NotNull CountDownLatch readWaiter = new CountDownLatch(0);

    // Constructor

    DirectionalStreamObserver(@NotNull DirectionalStream stream) {
        this.stream = stream;
    }

    // Modules

    void fireOperation(@NotNull Operation operation) {
        if (!stream.getConnection().isConnected()) {
            throw new IllegalStateException("Connection lost");
        }

        try {
            boolean offered = this.globalOperations.offer(operation, 1, TimeUnit.SECONDS);
            if (!offered) {
                throw new IllegalStateException("The queue of operations is full");
            }
        } catch (InterruptedException ignore) {

        }
    }

    void fireReading() {
        this.readWaiter.countDown();
    }

    boolean isWaitReading() {
        return readWaiter.getCount() > 0;
    }

    void setWaitReading(boolean waitReading) {
        if (waitReading) {
            this.readWaiter = new CountDownLatch(1);
            return;
        }

        this.readWaiter = new CountDownLatch(0);
    }

    public boolean awaitReading(int timeout, @NotNull TimeUnit unit) {
        try {
            if (stream.getQuicStream().getInputStream().available() > 0) {
                return true;
            }

            return this.readWaiter.await(timeout, unit);
        } catch (IOException e) {
            throw new AssertionError("Internal error");
        } catch (InterruptedException e) {
            return false;
        }
    }

    public @Nullable Operation awaitOperation(int timeout, @NotNull TimeUnit timeUnit) {
        try {
            return this.globalOperations.poll(timeout, timeUnit);
        } catch (InterruptedException e) {
            return null;
        }
    }
}