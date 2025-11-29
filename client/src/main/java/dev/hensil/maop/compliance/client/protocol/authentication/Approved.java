package dev.hensil.maop.compliance.client.protocol.authentication;

import dev.meinicke.semver.Version;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

public final class Approved extends Result {

    // Object

    private final @NotNull UUID sessionId;
    private final @NotNull String identifier;

    public Approved(@NotNull Version version, @NotNull String vendor, @NotNull UUID sessionId, @NotNull String identifier) {
        super(true, version, vendor);

        this.sessionId = sessionId;
        this.identifier = identifier;

        // Verifications
        if (identifier.getBytes(StandardCharsets.UTF_8).length > 0xFF) {
            throw new IllegalArgumentException("the identifier length cannot be higher than " + 0xFF + ".");
        }
    }

    // Getters

    public @NotNull UUID getSessionId() {
        return sessionId;
    }

    public @NotNull String getIdentifier() {
        return identifier;
    }

    // Implementations

    @Override
    public boolean equals(@Nullable Object object) {
        if (this == object) return true;
        if (!(object instanceof Approved approved)) return false;
        if (!super.equals(object)) return false;
        return Objects.equals(getSessionId(), approved.getSessionId()) && Objects.equals(getIdentifier(), approved.getIdentifier());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), getSessionId(), getIdentifier());
    }

    @Override
    public @NotNull String toString() {
        return "Approved{" +
                "sessionId=" + sessionId +
                ", identifier='" + identifier + '\'' +
                '}';
    }
}