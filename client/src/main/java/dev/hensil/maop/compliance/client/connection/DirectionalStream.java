package dev.hensil.maop.compliance.client.connection;

import java.io.Closeable;

public interface DirectionalStream extends Closeable {

    int getId();

    boolean isGlobal();

    void setGlobal(boolean isGlobal);

}