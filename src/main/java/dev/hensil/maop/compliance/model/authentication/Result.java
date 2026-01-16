package dev.hensil.maop.compliance.model.authentication;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import dev.hensil.maop.compliance.core.BidirectionalStream;

import dev.hensil.maop.compliance.model.Version;
import org.jetbrains.annotations.Blocking;
import org.jetbrains.annotations.MustBeInvokedByOverriders;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public sealed class Result permits Disapproved, Approved {

    // Static initializers

    public static @NotNull ByteBuffer allocate() {
        return ByteBuffer
                .allocate(1 +
                        (16 + 1 + Byte.MAX_VALUE) +
                        (2 + 4 + 2 + Short.MAX_VALUE) +
                        1 + Byte.MAX_VALUE + 1 + Byte.MAX_VALUE
                );
    }

    public static int MIN_LENGTH = 1 + (16 + 1 + 1) + (2 + 4 + 2 + 1) + 1 + 1 + 1 + 1;

    @Blocking
    public static @NotNull Result readResult(@NotNull BidirectionalStream stream, int timeout, @NotNull TimeUnit unit) throws IOException, TimeoutException {
        @NotNull CompletableFuture<Result> future = new CompletableFuture<>();
        future.orTimeout(timeout, unit);

        CompletableFuture.runAsync(() -> {
            try {
                int approvedByte = stream.readUnsignedByte();
                boolean approved = approvedByte == 1;

                UUID sessionId = null;
                String identifier = null;
                short code = 0;
                Duration retryAfter = null;
                String reason = null;

                if (approved) {
                    long most = stream.readLong();
                    long least = stream.readLong();
                    sessionId = new UUID(most, least);

                    int idLen = stream.readUnsignedByte();
                    byte[] idBytes = new byte[idLen];

                    stream.readFully(idBytes);

                    identifier = new String(idBytes, StandardCharsets.UTF_8);
                } else {
                    code = stream.readShort();
                    int ms = stream.readInt();

                    long msUnsigned = Integer.toUnsignedLong(ms);
                    if (msUnsigned > 0L) {
                        retryAfter = Duration.ofMillis(msUnsigned);
                    }

                    int reasonLen = stream.readUnsignedShort();
                    if (reasonLen > 0) {
                        byte[] reasonBytes = new byte[reasonLen];

                        stream.readFully(reasonBytes);

                        reason = new String(reasonBytes, StandardCharsets.UTF_8);
                    }
                }

                int versionLen = stream.readUnsignedByte();
                byte[] versionBytes = new byte[versionLen];

                stream.readFully(versionBytes);

                String versionString = new String(versionBytes, StandardCharsets.UTF_8);
                Version version;

                try {
                    version = Version.parse(versionString);
                } catch (Throwable e) {
                    throw new IOException("Invalid version", e);
                }

                int vendorLen = stream.readUnsignedByte();
                byte[] vendorBytes = new byte[vendorLen];

                stream.readFully(vendorBytes);

                String vendor = new String(vendorBytes, StandardCharsets.UTF_8);
                if (approved) {
                    future.complete(new Approved(version, vendor, sessionId, identifier));
                }

                future.complete(new Disapproved(version, vendor, code, reason, retryAfter));
            } catch (IOException e) {
                future.completeExceptionally(e);
            }
        });

        try {
            return future.join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof TimeoutException) {
                throw new TimeoutException("Time out: " + timeout + " " + unit.name().toLowerCase());
            }

            if (e.getCause() instanceof IOException io) {
                throw io;
            }

            throw new AssertionError("Internal error");
        }
    }

    @NotNull
    public static Result parse(@NotNull ByteBuffer buffer) {
        int approvedByte = Byte.toUnsignedInt(buffer.get());
        boolean approved = approvedByte == 1;
        UUID sessionId = null;
        String identifier = null;
        short code = 0;
        Duration retryAfter = null;
        String reason = null;
        if (approved) {
            long most = 0L;
            long least = 0L;

            for(int i = 0; i < 8; ++i) {
                most = most << 8 | Byte.toUnsignedLong(buffer.get());
            }

            for(int i = 0; i < 8; ++i) {
                least = least << 8 | Byte.toUnsignedLong(buffer.get());
            }

            sessionId = new UUID(most, least);
            int idLen = Byte.toUnsignedInt(buffer.get());
            byte[] idBytes = new byte[idLen];
            buffer.get(idBytes);
            identifier = new String(idBytes, StandardCharsets.UTF_8);
        } else {
            code = buffer.getShort();
            int ms = buffer.getInt();
            long msUnsigned = Integer.toUnsignedLong(ms);
            if (msUnsigned > 0L) {
                retryAfter = Duration.ofMillis(msUnsigned);
            }

            int reasonLen = Short.toUnsignedInt(buffer.getShort());
            if (reasonLen > 0) {
                byte[] reasonBytes = new byte[reasonLen];
                buffer.get(reasonBytes);
                reason = new String(reasonBytes, StandardCharsets.UTF_8);
            }
        }

        int versionLen = Byte.toUnsignedInt(buffer.get());
        byte[] versionBytes = new byte[versionLen];
        buffer.get(versionBytes);
        String versionString = new String(versionBytes, StandardCharsets.UTF_8);
        Version version = Version.parse(versionString);
        int vendorLen = Byte.toUnsignedInt(buffer.get());
        byte[] vendorBytes = new byte[vendorLen];
        buffer.get(vendorBytes);
        String vendor = new String(vendorBytes, StandardCharsets.UTF_8);
        return approved ? new Approved(version, vendor, sessionId, identifier) : new Disapproved(version, vendor, code, reason, retryAfter);
    }

    // Objects

    private final boolean approved;
    private final @NotNull Version version;
    private final @NotNull String vendor;

    public Result(boolean approved, @NotNull Version version, @NotNull String vendor) {
        this.approved = approved;
        this.version = version;
        this.vendor = vendor;
        if (version.toString().getBytes(StandardCharsets.UTF_8).length > 255) {
            throw new IllegalArgumentException("the version length cannot be higher than 255.");
        } else if (vendor.getBytes(StandardCharsets.UTF_8).length > 255) {
            throw new IllegalArgumentException("the vendor length cannot be higher than 255.");
        }
    }

    @MustBeInvokedByOverriders
    public long getLength() {
        return 1 + vendor.length() + version.toString().length();
    }

    public boolean isApproved() {
        return this.approved;
    }

    public @NotNull Version getVersion() {
        return this.version;
    }

    public @NotNull String getVendor() {
        return this.vendor;
    }

    @NotNull
    public ByteBuffer toByteBuffer() {
        byte[] versionBytes = this.getVersion().toString().getBytes(StandardCharsets.UTF_8);
        byte[] vendorBytes = this.getVendor().getBytes(StandardCharsets.UTF_8);
        if (versionBytes.length > 255) {
            throw new IllegalArgumentException("version length cannot be higher than 255.");
        } else if (vendorBytes.length > 255) {
            throw new IllegalArgumentException("vendor length cannot be higher than 255.");
        } else {
            ByteBuffer buffer;
            if (this.isApproved()) {
                Approved a = (Approved)this;
                byte[] identifierBytes = a.getIdentifier().getBytes(StandardCharsets.UTF_8);
                int size = 18 + identifierBytes.length + 1 + versionBytes.length + 1 + vendorBytes.length;
                buffer = ByteBuffer.allocate(size);
                buffer.put((byte)1);
                UUID sessionId = a.getSessionId();
                long most = sessionId.getMostSignificantBits();
                long least = sessionId.getLeastSignificantBits();
                buffer.putLong(most);
                buffer.putLong(least);
                buffer.put((byte)identifierBytes.length);
                buffer.put(identifierBytes);
            } else {
                Disapproved d = (Disapproved)this;
                byte[] reasonBytes = d.getReason() != null ? d.getReason().getBytes(StandardCharsets.UTF_8) : new byte[0];
                int size = 9 + reasonBytes.length + 1 + versionBytes.length + 1 + vendorBytes.length;
                buffer = ByteBuffer.allocate(size);
                buffer.put((byte)0);
                buffer.putShort(d.getErrorCode());
                Duration retry = d.getRetryAfter();
                long msLong = retry != null ? retry.toMillis() : 0L;
                if (msLong < 0L) {
                    msLong = 0L;
                }

                int ms = (int)Math.min(msLong, 4294967295L);
                buffer.putInt(ms);
                int rLen = reasonBytes.length;
                buffer.putShort((short)rLen);
                if (rLen > 0) {
                    buffer.put(reasonBytes);
                }
            }

            buffer.put((byte)versionBytes.length);
            buffer.put(versionBytes);
            buffer.put((byte)vendorBytes.length);
            buffer.put(vendorBytes);
            buffer.flip();
            return buffer;
        }
    }

    public boolean equals(@Nullable Object object) {
        if (this == object) {
            return true;
        } else if (!(object instanceof Result result)) {
            return false;
        } else {
            return this.isApproved() == result.isApproved() && Objects.equals(this.getVersion(), result.getVersion()) && Objects.equals(this.getVendor(), result.getVendor());
        }
    }

    public int hashCode() {
        return Objects.hash(this.isApproved(), this.getVersion(), this.getVendor());
    }
}
