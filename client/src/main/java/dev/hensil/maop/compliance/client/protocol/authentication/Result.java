package dev.hensil.maop.compliance.client.protocol.authentication;

import dev.hensil.maop.compliance.client.exception.InvalidVersionException;
import dev.meinicke.semver.Version;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;

public sealed class Result permits Approved, Disapproved {

    // Static initializers

    /**
     * @throws java.nio.BufferUnderflowException when the buffer end before a parse the result
     * */
    public static @NotNull Result parse(@NotNull ByteBuffer buffer) throws InvalidVersionException {
        // [ approved:1 ]
        int approvedByte = Byte.toUnsignedInt(buffer.get());
        boolean approved = (approvedByte == 1);

        @Nullable UUID sessionId = null;
        @Nullable String identifier = null;

        short code = 0;
        @Nullable Duration retryAfter = null;
        @Nullable String reason = null;

        if (approved) {
            // [session_id:16]
            long most = 0L;
            long least = 0L;

            for (int i = 0; i < 8; i++) {
                most = (most << 8) | Byte.toUnsignedLong(buffer.get());
            }
            for (int i = 0; i < 8; i++) {
                least = (least << 8) | Byte.toUnsignedLong(buffer.get());
            }

            sessionId = new UUID(most, least);

            // [ identifier_len:1 ]
            int idLen = Byte.toUnsignedInt(buffer.get());
            byte[] idBytes = new byte[idLen];
            buffer.get(idBytes);
            identifier = new String(idBytes, StandardCharsets.UTF_8);
        } else {
            // [ error_code:2 ]
            code = buffer.getShort();

            // [ retry_after_ms:4 ]
            int ms = buffer.getInt();
            long msUnsigned = Integer.toUnsignedLong(ms);
            if (msUnsigned > 0L) {
                retryAfter = Duration.ofMillis(msUnsigned);
            }

            // [ sanitized_reason_len:2 ]
            int reasonLen = Short.toUnsignedInt(buffer.getShort());
            if (reasonLen > 0) {
                byte[] reasonBytes = new byte[reasonLen];
                buffer.get(reasonBytes);
                reason = new String(reasonBytes, StandardCharsets.UTF_8);
            }
        }

        // [ version_len:1 ]
        int versionLen = Byte.toUnsignedInt(buffer.get());
        byte[] versionBytes = new byte[versionLen];
        buffer.get(versionBytes);
        @NotNull String versionString = new String(versionBytes, StandardCharsets.UTF_8);
        @NotNull Version version;

        try {
            version = Version.parse(versionString);
        } catch (IllegalArgumentException e) {
            throw new InvalidVersionException(e.getMessage());
        }

        // [ vendor_len:1 ]
        int vendorLen = Byte.toUnsignedInt(buffer.get());
        byte[] vendorBytes = new byte[vendorLen];
        buffer.get(vendorBytes);
        @NotNull String vendor = new String(vendorBytes, StandardCharsets.UTF_8);

        if (approved) {
            return new Approved(version, vendor, sessionId, identifier);
        } else {
            return new Disapproved(version, vendor, code, reason, retryAfter);
        }
    }

    // Object

    private final boolean approved;

    private final @NotNull Version version;
    private final @NotNull String vendor;

    public Result(boolean approved, @NotNull Version version, @NotNull String vendor) {
        this.approved = approved;
        this.version = version;
        this.vendor = vendor;

        // Verifications
        if (version.toString().getBytes(StandardCharsets.UTF_8).length > 0xFF) {
            throw new IllegalArgumentException("the version length cannot be higher than " + 0xFF + ".");
        } else if (vendor.getBytes(StandardCharsets.UTF_8).length > 0xFF) {
            throw new IllegalArgumentException("the vendor length cannot be higher than " + 0xFF + ".");
        }
    }

    // Getters

    public boolean isApproved() {
        return approved;
    }

    public @NotNull Version getVersion() {
        return version;
    }
    public @NotNull String getVendor() {
        return vendor;
    }

    // Modules

    public @NotNull ByteBuffer toByteBuffer() {
        // Common tail: version and vendor
        byte[] versionBytes = getVersion().toString().getBytes(StandardCharsets.UTF_8);
        byte[] vendorBytes = getVendor().getBytes(StandardCharsets.UTF_8);

        if (versionBytes.length > 0xFF) {
            throw new IllegalArgumentException("version length cannot be higher than " + 0xFF + ".");
        }
        if (vendorBytes.length > 0xFF) {
            throw new IllegalArgumentException("vendor length cannot be higher than " + 0xFF + ".");
        }

        @NotNull ByteBuffer buffer;
        int size;

        if (isApproved()) {
            @NotNull Approved a = (Approved) this;
            byte[] identifierBytes = a.getIdentifier().getBytes(StandardCharsets.UTF_8);

            size = 1                       // approved
                    + 16                   // session_id
                    + 1 + identifierBytes.length
                    + 1 + versionBytes.length
                    + 1 + vendorBytes.length;

            buffer = ByteBuffer.allocate(size);

            // [ approved:1 ]
            buffer.put((byte) 1);

            // [session_id:16]
            @NotNull UUID sessionId = a.getSessionId();
            long most = sessionId.getMostSignificantBits();
            long least = sessionId.getLeastSignificantBits();

            buffer.putLong(most);
            buffer.putLong(least);

            // [ identifier_len:1 ]
            buffer.put((byte) identifierBytes.length);

            // [ identifier:identifier_len ]
            buffer.put(identifierBytes);
        } else {
            @NotNull Disapproved d = (Disapproved) this;
            byte[] reasonBytes = d.getReason() != null ? d.getReason().getBytes(StandardCharsets.UTF_8) : new byte[0];

            size = 1                       // approved
                    + 2                   // error_code
                    + 4                   // retry_after_ms
                    + 2 + reasonBytes.length
                    + 1 + versionBytes.length
                    + 1 + vendorBytes.length;

            buffer = ByteBuffer.allocate(size);

            // [ approved:1 ]
            buffer.put((byte) 0);

            // [ error_code:2 ]
            buffer.putShort(d.getCode());

            // [ retry_after_ms:4 ]
            @Nullable Duration retry = d.getRetryAfter();

            long msLong = (retry != null ? retry.toMillis() : 0L);
            if (msLong < 0L) msLong = 0L;

            int ms = (int) Math.min(msLong, 0xFFFFFFFFL);
            buffer.putInt(ms);

            // [ sanitized_reason_len:2 ]
            int rLen = reasonBytes.length;
            buffer.putShort((short) rLen);

            // [sanitized_reason_utf8:reason_len]
            if (rLen > 0) buffer.put(reasonBytes);
        }

        // [ version_len:1 ]
        buffer.put((byte) versionBytes.length);

        // [ version_major:version_len ]
        buffer.put(versionBytes);

        // [ vendor_len:1 ]
        buffer.put((byte) vendorBytes.length);

        // [ vendor:vendor_len ]
        buffer.put(vendorBytes);

        buffer.flip();
        return buffer;
    }

    // Implementations

    @Override
    public boolean equals(@Nullable Object object) {
        if (this == object) return true;
        if (!(object instanceof Result result)) return false;
        return isApproved() == result.isApproved() && Objects.equals(getVersion(), result.getVersion()) && Objects.equals(getVendor(), result.getVendor());
    }
    @Override
    public int hashCode() {
        return Objects.hash(isApproved(), getVersion(), getVendor());
    }
}