package dev.hensil.maop.compliance.exception;

import java.io.IOException;

public class DirectionalStreamException extends IOException {
    public DirectionalStreamException(String message) {
        super(message);
    }

    public DirectionalStreamException(String message, Throwable cause) {
        super(message, cause);
    }
}
