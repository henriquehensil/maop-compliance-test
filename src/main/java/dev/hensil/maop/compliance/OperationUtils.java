package dev.hensil.maop.compliance;

import dev.hensil.maop.compliance.exception.GlobalOperationManagerException;
import dev.hensil.maop.compliance.model.*;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.DataInput;
import java.io.IOException;
import java.util.Set;

enum OperationUtils {

    // Static enums

    MESSAGE((byte) 0x00, 11) {
        @Override
        public @NotNull Operation readOperation(@NotNull DataInput dataInput) throws IOException {
            return null;
        }

        @Override
        public void globalHandle(@NotNull Operation operation, @NotNull Connection connection) throws GlobalOperationManagerException {
            throw new UnsupportedOperationException("Not global operation");
        }
    },
    REQUEST((byte) 0x01, 17) {
        @Override
        public @NotNull Operation readOperation(@NotNull DataInput dataInput) throws IOException {
            return null;
        }

        @Override
        public void globalHandle(@NotNull Operation operation, @NotNull Connection connection) throws GlobalOperationManagerException {
            throw new UnsupportedOperationException("Not global operation");
        }
    },
    RESPONSE((byte) 0x02, 20) {
        @Override
        public @NotNull Operation readOperation(@NotNull DataInput dataInput) throws IOException {
            return null;
        }

        @Override
        public void globalHandle(@NotNull Operation operation, @NotNull Connection connection) throws GlobalOperationManagerException {
            throw new UnsupportedOperationException("Not global operation");
        }
    },
    PROCEED((byte) 0x03, 2) {
        @Override
        public @NotNull Proceed readOperation(@NotNull DataInput dataInput) throws IOException {
            short count = dataInput.readShort();

            @NotNull Proceed.Entry[] entries = new Proceed.Entry[count];
            for (int i = 0; i < entries.length; i++) {
                entries[i] = new Proceed.Entry(dataInput.readLong());
            }

            return new Proceed(entries);
        }

        @Override
        public void globalHandle(@NotNull Operation operation, @NotNull Connection connection) throws GlobalOperationManagerException {
            if (!(operation instanceof Proceed proceed)) {
                throw new GlobalOperationManagerException("Not a fail operation: " + operation);
            }

            for (@NotNull Proceed.Entry entry : proceed.getEntries()) {
                boolean success = false;

                @NotNull GlobalOperationsManager manager = connection.getManager();
                @Nullable Set<GlobalOperationsManager.Stage> stages = manager.getStages(entry.getStream());
                if (stages == null) {
                    throw new GlobalOperationManagerException("There are no operations on stream '" + entry.getStream() + "'  being managed by the global manager");
                }

                for (@NotNull GlobalOperationsManager.Stage stage : stages) {
                    if (!stage.isFinished()) {
                        stage.fire(operation);
                        success = true;
                    }
                }

                if (!success) {
                    throw new GlobalOperationManagerException("All operations have already been completed on Stream '" + entry.getStream() + "' by the global manager");
                }
            }
        }
    },
    REFUSE((byte) 0x04, 2) {
        @Override
        public @NotNull Refuse readOperation(@NotNull DataInput dataInput) throws IOException {
            short count = dataInput.readShort();

            @NotNull Refuse.Entry @NotNull [] entries = new Refuse.Entry[count];
            for (int i = 0; i < entries.length; i++) {
                long stream = dataInput.readLong();
                int retry = dataInput.readInt();
                short reason = dataInput.readShort();

                entries[i] = new Refuse.Entry(stream, retry, reason);
            }

            return new Refuse(entries);
        }

        @Override
        public void globalHandle(@NotNull Operation operation, @NotNull Connection connection) throws GlobalOperationManagerException {
            if (!(operation instanceof Refuse refuse)) {
                throw new GlobalOperationManagerException("Not a fail operation: " + operation);
            }

            for (@NotNull Refuse.Entry entry : refuse.getEntries()) {
                boolean success = false;

                @NotNull GlobalOperationsManager manager = connection.getManager();
                @Nullable Set<GlobalOperationsManager.Stage> stages = manager.getStages(entry.getStream());
                if (stages == null) {
                    throw new GlobalOperationManagerException("There are no operations on stream '" + entry.getStream() + "'  being managed by the global manager");
                }

                for (@NotNull GlobalOperationsManager.Stage stage : stages) {
                    if (!stage.isFinished()) {
                        stage.fire(operation);
                        success = true;
                    }
                }

                if (!success) {
                    throw new GlobalOperationManagerException("All operations have already been completed on Stream '" + entry.getStream() + "' by the global manager");
                }
            }
        }
    },
    BLOCK((byte) 0x05, 4) {
        @Override
        public @NotNull Operation readOperation(@NotNull DataInput dataInput) throws IOException {
            return null;
        }

        @Override
        public void globalHandle(@NotNull Operation operation, @NotNull Connection connection) throws GlobalOperationManagerException {
            throw new UnsupportedOperationException("Not global operation");
        }
    },
    BLOCK_END((byte) 0x06, 8) {
        @Override
        public @NotNull Operation readOperation(@NotNull DataInput dataInput) throws IOException {
            return null;
        }

        @Override
        public void globalHandle(@NotNull Operation operation, @NotNull Connection connection) throws GlobalOperationManagerException {
            throw new UnsupportedOperationException("Not global operation");
        }
    },
    FAIL((byte) 0x07, 12) {
        @Override
        public @NotNull Fail readOperation(@NotNull DataInput dataInput) throws IOException {
            long stream = dataInput.readLong();
            short error = dataInput.readShort();
            @NotNull String reason = dataInput.readUTF();

            return new Fail(stream, error, reason);
        }

        @Override
        public void globalHandle(@NotNull Operation operation, @NotNull Connection connection) throws GlobalOperationManagerException {
            if (!(operation instanceof Fail fail)) {
                throw new GlobalOperationManagerException("Not a fail operation: " + operation);
            }

            boolean success = false;

            @NotNull GlobalOperationsManager manager = connection.getManager();
            @Nullable Set<GlobalOperationsManager.Stage> stages = manager.getStages(fail.getStream());
            if (stages == null) {
                throw new GlobalOperationManagerException("There are no operations on stream '" + fail.getStream() + "'  being managed by the global manager");
            }

            for (@NotNull GlobalOperationsManager.Stage stage : stages) {
                if (!stage.isFinished()) {
                    stage.fire(operation);
                    success = true;
                }
            }

            if (!success) {
                throw new GlobalOperationManagerException("All operations have already been completed on Stream '" + fail.getStream() + "' by the global manager");
            }
        }
    },
    DONE((byte) 0x08, 2) {
        @Override
        public @NotNull Done readOperation(@NotNull DataInput dataInput) throws IOException {
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

        @Override
        public void globalHandle(@NotNull Operation operation, @NotNull Connection connection) throws GlobalOperationManagerException {
            if (!(operation instanceof Done done)) {
                throw new GlobalOperationManagerException("Not a fail operation: " + operation);
            }

            @NotNull GlobalOperationsManager manager = connection.getManager();

            for (@NotNull Done.Entry entry : done.getEntries()) {
                @Nullable Set<GlobalOperationsManager.Stage> stages = manager.getStages(entry.getStream());
                if (stages == null) {
                    throw new GlobalOperationManagerException("There are no operations on stream '" + entry.getStream() + "'  being managed by the global manager");
                }

                boolean success = false;

                for (@NotNull GlobalOperationsManager.Stage stage : stages) {
                    if (!stage.isFinished()) {
                        stage.fire(operation);
                        success = true;
                    }
                }

                if (!success) {
                    throw new GlobalOperationManagerException("All operations have already been completed on Stream '" + entry.getStream() + "' by the global manager");
                }
            }
        }
    },
    DISCONNECT_REQUEST((byte) 0x09, 6) {
        @Override
        public @NotNull Operation readOperation(@NotNull DataInput dataInput) throws IOException {
            return null;
        }

        @Override
        public void globalHandle(@NotNull Operation operation, @NotNull Connection connection) throws GlobalOperationManagerException {
            throw new UnsupportedOperationException("Not global manageable");
        }
    },
    DISCONNECT((byte) 0x0A, 0) {
        @Override
        public @NotNull Operation readOperation(@NotNull DataInput dataInput) throws IOException {
            return null;
        }

        @Override
        public void globalHandle(@NotNull Operation operation, @NotNull Connection connection) throws GlobalOperationManagerException {
            throw new UnsupportedOperationException("Not global manageable");
        }
    };

    // Static initializers

    public static @Nullable OperationUtils getByCode(byte code) {
        return null;
    }

    // Objects

    private final byte code;
    private final int headerLength;

    OperationUtils(byte code, int headerLength) {
        this.code = code;
        this.headerLength = headerLength;
    }

    // Getters

    public byte getCode() {
        return code;
    }

    public int getHeaderLength() {
        return headerLength;
    }

    public boolean isGlobalManageable() {
        return this == PROCEED || this == REFUSE || this == DONE || this == FAIL;
    }

    // Modules

    public abstract @NotNull Operation readOperation(@NotNull DataInput dataInput) throws IOException;

    public abstract void globalHandle(@NotNull Operation operation, @NotNull Connection connection) throws GlobalOperationManagerException;
}