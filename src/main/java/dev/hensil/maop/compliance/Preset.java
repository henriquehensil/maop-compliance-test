package dev.hensil.maop.compliance;

import dev.hensil.maop.compliance.model.Version;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigInteger;
import java.net.URI;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.LinkedHashMap;
import java.util.Map;

public final class Preset {

    // Static initializers

    public static @NotNull Builder newBuilder() {
        return new Builder();
    }

    // Objects

    private final @NotNull String authenticationType;
    private final byte @NotNull [] authenticationToken;
    private final @NotNull Map<String, String> authenticationMetadata;

    private final @NotNull Version version;
    private final @NotNull String vendor;

    private final @NotNull URI host;
    private final @Nullable KeyStore keyStore;
    private final @Nullable String keyPassword;
    private final @Nullable X509Certificate certificate;
    private final @Nullable PrivateKey privateKey;

    // Constructor

    public Preset(
            @NotNull String authenticationType,
            byte @NotNull [] authenticationToken,
            @NotNull Map<String, String> authenticationMetadata,
            @NotNull Version version,
            @NotNull String vendor,
            @NotNull URI host,
            @Nullable KeyStore keyStore,
            @Nullable String keyPassword,
            @Nullable X509Certificate certificate,
            @Nullable PrivateKey privateKey
    ) {
        this.authenticationType = authenticationType;
        this.authenticationToken = authenticationToken;
        this.authenticationMetadata = authenticationMetadata;
        this.version = version;
        this.vendor = vendor;
        this.host = host;
        this.keyStore = keyStore;
        this.keyPassword = keyPassword;
        this.certificate = certificate;
        this.privateKey = privateKey;
    }

    // Getters

    public @NotNull String getAuthenticationType() {
        return authenticationType;
    }

    public byte @NotNull [] getAuthenticationToken() {
        return authenticationToken;
    }

    public @NotNull Map<String, String> getAuthenticationMetadata() {
        return authenticationMetadata;
    }

    public @NotNull Version getVersion() {
        return version;
    }

    public @NotNull String getVendor() {
        return vendor;
    }

    public @NotNull URI getHost() {
        return host;
    }

    public boolean isServerNoCertification() {
        return (keyStore == null && keyPassword == null) && (certificate == null && privateKey == null);
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

        private @NotNull String authenticationType = "Basic";
        private byte @NotNull [] authenticationToken = new byte[0];
        private @NotNull Map<String, String> authenticationMetadata = new LinkedHashMap<>();

        private @NotNull Version version = Version.parse("1.0.0");
        private @Nullable String vendor;

        private @Nullable URI host;

        private @Nullable KeyStore keyStore;
        private @Nullable String keyPassword;

        private @Nullable X509Certificate certificate;
        private @Nullable PrivateKey privateKey;

        // Constructor

        private Builder() {
            //
        }

        // Modules

        public @NotNull Builder authenticationType(@NotNull String type) {
            this.authenticationType = type;
            return this;
        }

        public @NotNull Builder authenticationToken(byte @NotNull [] token) {
            this.authenticationToken = token;
            return this;
        }

        public @NotNull Builder authenticationMetadata(@NotNull String key, @NotNull String value) {
            this.authenticationMetadata.put(key, value);
            return this;
        }

        public @NotNull Builder authenticationMetadata(@NotNull Map<String, String> data) {
            this.authenticationMetadata = data;
            return this;
        }

        public @NotNull Builder version(@NotNull Version version) {
            this.version = version;
            return this;
        }

        public @NotNull Builder vendor(@NotNull String vendor) {
            this.vendor = vendor;
            return this;
        }

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

            if (vendor == null) {
                throw new IllegalArgumentException("Vendor cannot be empty");
            }

            return new Preset(
                    authenticationType,
                    authenticationToken,
                    authenticationMetadata,
                    version,
                    vendor,
                    host,
                    keyStore,
                    keyPassword,
                    certificate,
                    privateKey
            );
        }
    }
}