package dev.hensil.maop.compliance.core;

import dev.hensil.maop.compliance.model.operation.*;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.DataInput;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class OperationUtil {

    // Static initializers

    public static final @NotNull OperationUtil MESSAGE = new OperationUtil((byte) 0x00, Message.class, 11) {
        @Override
        void handleObserve(@NotNull Operation operation, @NotNull Connection connection) {
            throw new UnsupportedOperationException();
        }

        @Override
        public @NotNull Operation read(@NotNull DataInput dataInput) throws IOException {
            return null;
        }
    };

    public static final @NotNull OperationUtil REQUEST = new OperationUtil((byte) 0x01, Request.class, 17) {
        @Override
        void handleObserve(@NotNull Operation operation, @NotNull Connection connection) {
            throw new UnsupportedOperationException();
        }

        @Override
        public @NotNull Operation read(@NotNull DataInput dataInput) throws IOException {
            return null;
        }
    };

    public static final @NotNull OperationUtil RESPONSE = new OperationUtil((byte) 0x02, Response.class, 20) {
        @Override
        void handleObserve(@NotNull Operation operation, @NotNull Connection connection) {
            throw new UnsupportedOperationException();
        }

        @Override
        public @NotNull Response read(@NotNull DataInput dataInput) throws IOException {
            long payload = dataInput.readLong();
            long execStart = dataInput.readLong();
            int execTime = dataInput.readInt();

            return new Response(payload, execStart, execTime);
        }
    };

    public static final @NotNull OperationUtil PROCEED = new OperationUtil((byte) 0x03, Proceed.class, 2) {
        @Override
        void handleObserve(@NotNull Operation operation, @NotNull Connection connection) {
            if (!(operation instanceof Proceed proceed)) {
                throw new ClassCastException("Not proceed operation: " + operation);
            }

            for (@NotNull Proceed.Entry entry : proceed.getEntries()) {
                long streamId = entry.getStream();
                @Nullable DirectionalStream stream = connection.getDirectionalStream(streamId);
                if (stream != null) {
                    @Nullable DirectionalStreamObserver observer = connection.getObserver(stream);
                    if (observer != null) {
                        observer.fireOperation(proceed);
                    }
                }
            }
        }

        @Override
        public @NotNull Operation read(@NotNull DataInput dataInput) throws IOException {
            short count = dataInput.readShort();

            @NotNull Proceed.Entry[] entries = new Proceed.Entry[count];
            for (int i = 0; i < entries.length; i++) {
                entries[i] = new Proceed.Entry(dataInput.readLong());
            }

            return new Proceed(entries);
        }
    };

    public static final @NotNull OperationUtil REFUSE = new OperationUtil((byte) 0x04, Refuse.class, 2) {
        @Override
        void handleObserve(@NotNull Operation operation, @NotNull Connection connection) throws ClassCastException {
            if (!(operation instanceof Refuse refuse)) {
                throw new ClassCastException("Not a fail operation: " + operation);
            }

            for (@NotNull Refuse.Entry entry : refuse.getEntries()) {
                long streamId = entry.getStream();
                @Nullable DirectionalStream stream = connection.getDirectionalStream(streamId);
                if (stream != null) {
                    @Nullable DirectionalStreamObserver observer = connection.getObserver(stream);
                    if (observer != null) {
                        observer.fireOperation(refuse);
                    }
                }
            }
        }

        @Override
        public @NotNull Refuse read(@NotNull DataInput dataInput) throws IOException {
            short count = dataInput.readShort();

            @NotNull List<Refuse.Entry> entries = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                long streamId = dataInput.readLong();
                int retryAfter = dataInput.readInt();
                short reasonCode = dataInput.readShort();

                entries.add(new Refuse.Entry(streamId, retryAfter, reasonCode));
            }

            return new Refuse(entries.toArray(new Refuse.Entry[0]));
        }
    };

    public static final @NotNull OperationUtil BLOCK = new OperationUtil((byte) 0x05, Block.class, 4) {
        @Override
        void handleObserve(@NotNull Operation operation, @NotNull Connection connection) {
            throw new UnsupportedOperationException();
        }

        @Override
        public @NotNull Block read(@NotNull DataInput dataInput) throws IOException {
            int payload = dataInput.readInt();
            byte @NotNull [] bytes = new byte[payload];
            dataInput.readFully(bytes);

            return new Block(bytes);
        }
    };

    public static final @NotNull OperationUtil BLOCK_END = new OperationUtil((byte) 0x06, BlockEnd.class, 8) {
        @Override
        void handleObserve(@NotNull Operation operation, @NotNull Connection connection) {
            throw new UnsupportedOperationException();
        }

        @Override
        public @NotNull BlockEnd read(@NotNull DataInput dataInput) throws IOException {
            return new BlockEnd(dataInput.readLong());
        }
    };

    public static final @NotNull OperationUtil FAIL = new OperationUtil((byte) 0x07, Fail.class, 12) {
        @Override
        void handleObserve(@NotNull Operation operation, @NotNull Connection connection) throws ClassCastException {
            if (!(operation instanceof Fail fail)) {
                throw new ClassCastException("Not a fail operation: " + operation);
            }

            long streamId = fail.getStream();
            @Nullable DirectionalStream stream = connection.getDirectionalStream(streamId);
            if (stream != null) {
                @Nullable DirectionalStreamObserver observer = connection.getObserver(stream);
                if (observer != null) {
                    observer.fireOperation(fail);
                }
            }
        }

        @Override
        public @NotNull Operation read(@NotNull DataInput dataInput) throws IOException {
            long stream = dataInput.readLong();
            short error = dataInput.readShort();
            @NotNull String reason = dataInput.readUTF();

            return new Fail(stream, error, reason);
        }
    };

    public static final @NotNull OperationUtil DONE = new OperationUtil((byte) 0x08, Done.class, 2) {
        @Override
        void handleObserve(@NotNull Operation operation, @NotNull Connection connection) throws ClassCastException {
            if (!(operation instanceof Done done)) {
                throw new ClassCastException("Not a Done operation: " + operation);
            }

            for (@NotNull Done.Entry entry : done.getEntries()) {
                long streamId = entry.getStream();
                @Nullable DirectionalStream stream = connection.getDirectionalStream(streamId);
                if (stream != null) {
                    @Nullable DirectionalStreamObserver observer = connection.getObserver(stream);
                    if (observer != null) {
                        observer.fireOperation(done);
                    }
                }
            }
        }

        @Override
        public @NotNull Operation read(@NotNull DataInput dataInput) throws IOException {
            short count = dataInput.readShort();

            @NotNull Done.Entry @NotNull [] entries = new Done.Entry[count];
            for (int i = 0; i < entries.length; i++) {
                long stream = dataInput.readLong();
                int start = dataInput.readInt();
                int end = dataInput.readInt();

                entries[i] = new Done.Entry(stream, start, end);
            }

            return new Done(entries);
        }
    };

    public static final @NotNull OperationUtil DISCONNECT_REQUEST = new OperationUtil((byte) 0x09, DisconnectRequest.class, 6) {
        @Override
        void handleObserve(@NotNull Operation operation, @NotNull Connection connection) {
            throw new UnsupportedOperationException();
        }

        @Override
        public @NotNull Operation read(@NotNull DataInput dataInput) throws IOException {
            return null;
        }
    };

    public static final @NotNull OperationUtil DISCONNECT = new OperationUtil((byte) 0x0A, Disconnect.class, 0) {
        @Override
        void handleObserve(@NotNull Operation operation, @NotNull Connection connection) {
            throw new UnsupportedOperationException();
        }

        @Override
        public @NotNull Operation read(@NotNull DataInput dataInput) throws IOException {
            return null;
        }
    };

    private static final @NotNull Map<Byte, OperationUtil> map = new HashMap<>();

    static {
        for (@NotNull Field field : OperationUtil.class.getFields()) {
            if (field.getDeclaringClass() == OperationUtil.class &&
                    Modifier.isStatic(field.getModifiers()) &&
                    Modifier.isPublic(field.getModifiers())
            ) {
                try {
                    @NotNull OperationUtil util = (OperationUtil) field.get(null);
                    map.put(util.code, util);
                } catch (Throwable e) {
                    throw new AssertionError("Internal error", e);
                }
            }
        }
    }

    public static @Nullable OperationUtil getByCode(byte code) {
        return map.get(code);
    }

    // Objects

    private final byte code;
    private final @NotNull Class<? extends Operation> reference;
    private final int headerLength;

    protected OperationUtil(byte code, @NotNull Class<? extends Operation> reference, int headerLength) {
        this.code = code;
        this.reference = reference;
        this.headerLength = headerLength;
    }

    public final @NotNull String getName() {
        return reference.getSimpleName();
    }

    public int getHeaderLength() {
        return headerLength;
    }

    public final short getCode() {
        return code;
    }

    public final boolean matches(@NotNull Operation operation) {
        return code == operation.getCode() && reference == operation.getClass();
    }

    public final boolean isGlobalOperation() {
        return reference == Refuse.class ||
                reference == Proceed.class ||
                reference == Fail.class ||
                reference == Done.class ||
                reference == Disconnect.class ||
                reference == DisconnectRequest.class;
    }

    @Override
    public String toString() {
        return getName();
    }

    // Abstract

    /**
     * @throws ClassCastException if operation is not a valid operation for this util
     * */
    abstract void handleObserve(@NotNull Operation operation, @NotNull Connection connection);

    /**
     * @throws UnsupportedOperationException if this operation util is not observable
     * */
    public abstract @NotNull Operation read(@NotNull DataInput dataInput) throws IOException;

}