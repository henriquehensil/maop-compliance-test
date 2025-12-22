package dev.hensil.maop.compliance;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URI;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;

public class Preset {

    // Static initializers

    public static @NotNull Builder newBuilder() {
        return new Builder();
    }

    // Objects

    private final @NotNull URI host;
    private final @Nullable KeyStore keyStore;
    private final @Nullable String keyPassword;
    private final @Nullable X509Certificate certificate;
    private final @Nullable PrivateKey privateKey;

    // Constructor

    public Preset(@NotNull URI host) {
        this(host, null, null, null, null);
    }

    public Preset(
            @NotNull URI host,
            @Nullable KeyStore keyStore,
            @Nullable String keyPassword,
            @Nullable X509Certificate certificate,
            @Nullable PrivateKey privateKey
    ) {
        this.host = host;
        this.keyStore = keyStore;
        this.keyPassword = keyPassword;
        this.certificate = certificate;
        this.privateKey = privateKey;
    }

    // Getters

    public @NotNull URI getHost() {
        return host;
    }

    public boolean isServerNoCertification() {
        return (keyStore == null && keyPassword == null) || (certificate == null && privateKey == null);
    }

    public @Nullable KeyStore getKeyStore() {
        return keyStore;
    }

    public @Nullable String getKeyPassword() {
        return keyPassword;
    }

    public @Nullable X509Certificate getCertificate() {
        return certificate;
    }

    public @Nullable PrivateKey getPrivateKey() {
        return privateKey;
    }

    // Classes

    public static final class Builder {

        private @Nullable URI host;

        private @Nullable KeyStore keyStore;
        private @Nullable String keyPassword;

        private @Nullable X509Certificate certificate;
        private @Nullable PrivateKey privateKey;

        // Constructor

        private Builder() {

        }

        // Modules

        public @NotNull Builder uri(@Nullable URI uri) {
            this.host = uri;
            return this;
        }

        public @NotNull Builder keyStore(@Nullable KeyStore keyStore) {
            this.keyStore = keyStore;
            return this;
        }

        public @NotNull Builder keyPassword(@Nullable String keyPassword) {
            this.keyPassword = keyPassword;
            return this;
        }

        public @NotNull Builder certificate(@Nullable X509Certificate certificate) {
            this.certificate = certificate;
            return this;
        }

        public @NotNull Builder privateKey(@Nullable PrivateKey privateKey) {
            this.privateKey = privateKey;
            return this;
        }

        public @NotNull Preset build() {
            if (this.host == null) {
                throw new IllegalStateException("Cannot create connection when URI is not set");
            } else if (this.certificate != null && this.keyStore != null) {
                throw new IllegalArgumentException("Cannot set both client certificate and key manager");
            } else if (this.certificate != null && this.privateKey == null) {
                throw new IllegalArgumentException("Client certificate key must be set when client certificate is set");
            } else if (this.keyStore != null && this.keyPassword == null) {
                throw new IllegalArgumentException("Key password must be set when key manager is set");
            }

            return new Preset(host, keyStore, keyPassword, certificate, privateKey);
        }
    }
}
