package dev.hensil.maop.compliance.model.authentication;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

import dev.hensil.maop.compliance.model.Version;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class Approved extends Result {

    private final @NotNull UUID sessionId;
    private final @NotNull String identifier;

    public Approved(Version version, @NotNull String vendor, @NotNull UUID sessionId, @NotNull String identifier) {
        super(true, version, vendor);
        this.sessionId = sessionId;
        this.identifier = identifier;
        if (identifier.getBytes(StandardCharsets.UTF_8).length > 255) {
            throw new IllegalArgumentException("the identifier length cannot be higher than 255.");
        }
    }

    public @NotNull UUID getSessionId() {
        return this.sessionId;
    }

    public @NotNull String getIdentifier() {
        return this.identifier;
    }

    @Override
    public long getLength() {
        return super.getLength() + 16 + identifier.length();
    }

    public boolean equals(@Nullable Object object) {
        if (this == object) {
            return true;
        } else if (object instanceof Approved approved) {
            if (!super.equals(object)) {
                return false;
            } else {
                return Objects.equals(this.getSessionId(), approved.getSessionId()) && Objects.equals(this.getIdentifier(), approved.getIdentifier());
            }
        } else {
            return false;
        }
    }

    public int hashCode() {
        return Objects.hash(new Object[]{super.hashCode(), this.getSessionId(), this.getIdentifier()});
    }

    public @NotNull String toString() {
        String var10000 = String.valueOf(this.sessionId);
        return "Approved{sessionId=" + var10000 + ", identifier='" + this.identifier + "'}";
    }
}