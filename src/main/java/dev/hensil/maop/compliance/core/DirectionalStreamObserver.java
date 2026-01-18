package dev.hensil.maop.compliance.core;

import dev.hensil.maop.compliance.model.operation.Operation;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

final class DirectionalStreamObserver {

    // Objects

    private final @NotNull DirectionalStream stream;
    private final @NotNull LinkedBlockingQueue<Operation> globalOperations = new LinkedBlockingQueue<>(30);

    private final @NotNull AtomicLong count = new AtomicLong(0);
    private volatile long untilAvailable = 0;
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

    void fireReading(long newBytes) {
        if (!isWaitReading()) {
            return;
        }

        long total = this.count.addAndGet(newBytes);
        if (total >= untilAvailable) {
            this.readWaiter.countDown();
            resetReading();
        }
    }

    boolean isWaitReading() {
        return readWaiter.getCount() > 0;
    }

    private void setWaitReading(boolean waitReading) {
        if (waitReading && !isWaitReading()) {
            this.readWaiter = new CountDownLatch(1);
        }
    }

    private void resetReading() {
        setWaitReading(false);

        this.untilAvailable = 0;
        this.count.set(0);
    }

    void setUntilAvailable(long bytes) {
        if (isWaitReading()) {
            throw new IllegalStateException("Already waiting for available bytes: " + untilAvailable);
        }

        setWaitReading(true);
        this.untilAvailable = bytes;
    }

    public boolean awaitReading(int timeout, @NotNull TimeUnit unit) {
        if (!isWaitReading()) {
            return false;
        }

        try {
            return this.readWaiter.await(timeout, unit);
        } catch (InterruptedException e) {
            return false;
        } finally {
            resetReading();
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