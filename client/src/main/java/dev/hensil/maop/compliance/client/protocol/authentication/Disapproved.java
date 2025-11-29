package dev.hensil.maop.compliance.client.protocol.authentication;

import dev.meinicke.semver.Version;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;

public final class Disapproved extends Result {

    // Object

    private final short code;
    private final @Nullable String reason;

    private final @Nullable Duration retryAfter;

    public Disapproved(@NotNull Version version, @NotNull String vendor, short code, @Nullable String reason, @Nullable Duration retryAfter) {
        super(false, version, vendor);

        this.code = code;
        this.reason = reason;
        this.retryAfter = retryAfter;

        // Verifications
        if (reason != null && reason.getBytes(StandardCharsets.UTF_8).length > 0xFFFF) {
            throw new IllegalArgumentException("the reason length cannot be higher than " + 0xFFFF + ".");
        }
    }

    // Getters

    public short getCode() {
        return code;
    }
    public @Nullable String getReason() {
        return reason;
    }

    public @Nullable Duration getRetryAfter() {
        return retryAfter;
    }

    // Implementations

    @Override
    public boolean equals(@Nullable Object object) {
        if (this == object) return true;
        if (!(object instanceof Disapproved that)) return false;
        if (!super.equals(object)) return false;
        return getCode() == that.getCode() && Objects.equals(getReason(), that.getReason()) && Objects.equals(getRetryAfter(), that.getRetryAfter());
    }
    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), getCode(), getReason(), getRetryAfter());
    }

    @Override
    public @NotNull String toString() {
        return "Disapproved{" +
                "code=" + code +
                ", reason='" + reason + '\'' +
                ", retryAfter=" + retryAfter +
                '}';
    }
}