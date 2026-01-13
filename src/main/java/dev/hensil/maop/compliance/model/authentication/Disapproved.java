package dev.hensil.maop.compliance.model.authentication;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;

import dev.hensil.maop.compliance.model.Version;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class Disapproved extends Result {

    private final short code;
    private final @Nullable String reason;
    private final @Nullable Duration retryAfter;

    public Disapproved(@NotNull Version version, @NotNull String vendor, short code, @Nullable String reason, @Nullable Duration retryAfter) {
        super(false, version, vendor);
        this.code = code;
        this.reason = reason;
        this.retryAfter = retryAfter;

        if (reason != null && reason.getBytes(StandardCharsets.UTF_8).length > 65535) {
            throw new IllegalArgumentException("the reason length cannot be higher than 65535.");
        }
    }

    public short getErrorCode() {
        return this.code;
    }

    public @Nullable String getReason() {
        return this.reason;
    }

    public @Nullable Duration getRetryAfter() {
        return this.retryAfter;
    }

    @Override
    public long getLength() {
        return super.getLength() + 2 + (reason != null ? reason.length() : 0) + (retryAfter != null ? 4 : 0);
    }

    public boolean equals(@Nullable Object object) {
        if (this == object) {
            return true;
        } else if (object instanceof Disapproved that) {
            if (!super.equals(object)) {
                return false;
            } else {
                return this.getErrorCode() == that.getErrorCode() && Objects.equals(this.getReason(), that.getReason()) && Objects.equals(this.getRetryAfter(), that.getRetryAfter());
            }
        } else {
            return false;
        }
    }

    public int hashCode() {
        return Objects.hash(super.hashCode(), this.getErrorCode(), this.getReason(), this.getRetryAfter());
    }

    public @NotNull String toString() {
        return "Disapproved{code=" + this.code + ", reason='" + this.reason + "', retryAfter=" + this.retryAfter + "}";
    }
}