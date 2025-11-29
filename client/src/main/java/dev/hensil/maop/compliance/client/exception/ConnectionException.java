package dev.hensil.maop.compliance.client.exception;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;

public class ConnectionException extends IOException {

    public ConnectionException(String message) {
        super(message);
    }

    public ConnectionException(String message, Throwable cause) {
        super(message, cause);
    }

    @Override
    public @NotNull String toString() {
        @NotNull String message = this.getMessage();
        @Nullable String causeName = this.getCause() != null ? this.getCause().getClass().getSimpleName() : null;

        return "\"" + message + "\"" + (causeName != null ? " cause: " + causeName + ": \"" + getCause().getMessage() + "\"" : "");
    }
}