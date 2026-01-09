package dev.hensil.maop.compliance.exception;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;

public class ConnectionException extends IOException {
    public ConnectionException(@NotNull Throwable cause) {
        super(cause);
    }

    public ConnectionException(String message) {
        super(message);
    }
}
