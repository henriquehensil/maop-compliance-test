package dev.hensil.maop.compliance;

import dev.hensil.maop.compliance.exception.GlobalOperationManagerException;
import dev.hensil.maop.compliance.model.*;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

final class GlobalOperationsManager {

    private final @NotNull Map<Long, Set<Stage>> operations = new ConcurrentHashMap<>();

    // Constructor

    GlobalOperationsManager() {
    }

    // Modules

    public @NotNull Stage manage(@NotNull DirectionalStream stream, @NotNull Message message) {
        for (@NotNull Set<Stage> stages : operations.values()) {
            for (@NotNull Stage stage : stages) {
                if (stage.operation == message) {
                    return stage;
                }
            }
        }

        @NotNull Stage stage = new Stage(message);
        @NotNull Set<Stage> set = this.operations.getOrDefault(stream.getId(), new HashSet<>());
        set.add(stage);
        this.operations.put(stream.getId(), set);

        return stage;
    }

    public @NotNull Stage manage(@NotNull DirectionalStream stream, @NotNull Request request) {
        for (@NotNull Set<Stage> stages : operations.values()) {
            for (@NotNull Stage stage : stages) {
                if (stage.operation == request) {
                    return stage;
                }
            }
        }

        @NotNull Stage stage = new Stage(request);
        @NotNull Set<Stage> set = this.operations.getOrDefault(stream.getId(), new HashSet<>());
        set.add(stage);
        this.operations.put(stream.getId(), set);

        return stage;
    }

    @Nullable Set<Stage> getStages(long stream) {
        return this.operations.get(stream);
    }

    // Classes

    public static final class Stage {

        private final @NotNull Operation operation;
        private @Nullable Operation authorization;
        private @Nullable Operation finish;

        private final @Nullable CountDownLatch authorizationWaiter;
        private final @NotNull CountDownLatch finishWaiter = new CountDownLatch(1);

        private final @NotNull Set<Class<? extends Operation>> supportedAuthorizations = new HashSet<>(2, 1f);
        private final @NotNull Set<Class<? extends Operation>> supportedFinishes = new HashSet<>(2, 1f) {{
            this.add(Fail.class);
            // Done or Response
        }};

        // Constructor

        private Stage(@NotNull Message message) {
            this.operation = message;

            if (message.getPayload() > 0) {
                this.authorizationWaiter = new CountDownLatch(1);
                this.supportedAuthorizations.add(Proceed.class);
                this.supportedAuthorizations.add(Refuse.class);
            } else {
                this.authorizationWaiter = null;
            }

            this.supportedFinishes.add(Done.class);
        }

        private Stage(@NotNull Request request) {
            this.operation = request;

            if (request.getPayload() > 0) {
                this.authorizationWaiter = new CountDownLatch(1);
                this.supportedAuthorizations.add(Proceed.class);
                this.supportedAuthorizations.add(Refuse.class);
            } else {
                this.authorizationWaiter = null;
            }

            this.supportedFinishes.add(Response.class);
        }

        // Modules

        void fire(@NotNull Operation operation) throws GlobalOperationManagerException {
            if (finish != null) {
                throw new GlobalOperationManagerException("This operation stage already finish: " + finish);
            }

            if (authorizationWaiter != null && authorization == null) {
                if (!supportedAuthorizations.contains(operation.getClass())) {
                    throw new GlobalOperationManagerException("Illegal operation context: " + operation);
                }

                this.authorizationWaiter.countDown();
                this.authorization = operation;

                return;
            }

            if (!supportedFinishes.contains(operation.getClass())) {
                throw new GlobalOperationManagerException("Illegal operation context: " + operation);
            }

            this.finish = operation;
            this.finishWaiter.countDown();
        }

        public @NotNull Operation await(int timeout) throws TimeoutException, InterruptedException {
            if (finish != null) {
                return finish;
            }

            if (authorizationWaiter != null) {
                boolean complete = this.authorizationWaiter.await(timeout, TimeUnit.MILLISECONDS);
                if (!complete) {
                    throw new TimeoutException();
                }

                if (authorization == null) {
                    throw new AssertionError("Internal error");
                }

                return authorization;
            }

            boolean complete = this.finishWaiter.await(timeout, TimeUnit.MILLISECONDS);
            if (!complete) {
                throw new TimeoutException();
            }

            if (finish == null) {
                throw new AssertionError("Internal error");
            }

            return finish;
        }

        public boolean isFinished() {
            return finish != null;
        }
    }
}