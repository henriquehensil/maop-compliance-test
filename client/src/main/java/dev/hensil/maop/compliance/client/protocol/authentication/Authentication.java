package dev.hensil.maop.compliance.client.protocol.authentication;

import dev.meinicke.semver.Version;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Unmodifiable;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

public final class Authentication {

    // Static initializers

    public static @NotNull Authentication parse(@NotNull ByteBuffer buffer) {
        // Type
        int typeLen = Byte.toUnsignedInt(buffer.get());
        byte[] typeBytes = new byte[typeLen];
        buffer.get(typeBytes);
        @NotNull String type = new String(typeBytes, StandardCharsets.UTF_8);

        // Token
        int tokenLen = Short.toUnsignedInt(buffer.getShort());
        byte[] token = new byte[tokenLen];
        buffer.get(token);

        // Metadata
        int pairs = Byte.toUnsignedInt(buffer.get());
        @NotNull Map<String, String> metadata = new LinkedHashMap<>(pairs);

        for (int i = 0; i < pairs; i++) {
            int keyLen = Short.toUnsignedInt(buffer.getShort());
            byte[] keyBytes = new byte[keyLen];
            buffer.get(keyBytes);
            @NotNull String key = new String(keyBytes, StandardCharsets.UTF_8);

            int valueLen = Short.toUnsignedInt(buffer.getShort());
            byte[] valueBytes = new byte[valueLen];
            buffer.get(valueBytes);
            @NotNull String value = new String(valueBytes, StandardCharsets.UTF_8);

            metadata.put(key, value);
        }

        // Version
        int versionLen = Byte.toUnsignedInt(buffer.get());
        byte[] versionBytes = new byte[versionLen];
        buffer.get(versionBytes);
        @NotNull String versionString = new String(versionBytes, StandardCharsets.UTF_8);
        @NotNull Version version = Version.parse(versionString);

        // Vendor
        int vendorLen = Byte.toUnsignedInt(buffer.get());
        byte[] vendorBytes = new byte[vendorLen];
        buffer.get(vendorBytes);
        @NotNull String vendor = new String(vendorBytes, StandardCharsets.UTF_8);

        // Finish
        return new Authentication(type, token, metadata, version, vendor);
    }

    // Object

    private final @NotNull String type;
    private final byte[] token;
    private final @NotNull Map<String, String> metadata;
    private final @NotNull Version version;
    private final @NotNull String vendor;

    public Authentication(@NotNull String type, byte[] token, @NotNull Map<String, String> metadata, @NotNull Version version, @NotNull String vendor) {
        this.type = type;
        this.token = token;
        this.metadata = Map.copyOf(metadata);
        this.version = version;
        this.vendor = vendor;

        // Verifications
        if (type.getBytes(StandardCharsets.UTF_8).length > 0xFF) {
            throw new IllegalArgumentException("the type length cannot be higher than " + 0xFF + " characters.");
        } else if (token.length > 0xFFFF) {
            throw new IllegalArgumentException("the token length cannot be higher than " + 0xFFFF + " bytes.");
        } else if (metadata.size() > 0xFF) {
            throw new IllegalArgumentException("the metadata size cannot be higher than " + 0xFF + ".");
        } else if (metadata.keySet().stream().anyMatch(key -> key.getBytes(StandardCharsets.UTF_8).length > 0xFFFF)) {
            throw new IllegalArgumentException("the metadata key length cannot be higher than " + 0xFFFF + ".");
        } else if (metadata.values().stream().anyMatch(value -> value.getBytes(StandardCharsets.UTF_8).length > 0xFFFF)) {
            throw new IllegalArgumentException("the metadata value length cannot be higher than " + 0xFFFF + ".");
        } else if (version.toString().getBytes(StandardCharsets.UTF_8).length > 0xFF) {
            throw new IllegalArgumentException("the version length cannot be higher than " + 0xFF + ".");
        } else if (vendor.getBytes(StandardCharsets.UTF_8).length > 0xFF) {
            throw new IllegalArgumentException("the vendor length cannot be higher than " + 0xFF + ".");
        }
    }

    // Getters

    public @NotNull String getType() {
        return type;
    }

    public byte[] getToken() {
        return token;
    }

    public @Unmodifiable @NotNull Map<String, String> getMetadata() {
        return metadata;
    }

    public @NotNull Version getVersion() {
        return version;
    }

    public @NotNull String getVendor() {
        return vendor;
    }

    // Modules

    public @NotNull ByteBuffer toByteBuffer() {
        // Variables
        byte[] type = getType().getBytes(StandardCharsets.UTF_8);
        byte[] version = getVersion().toString().getBytes(StandardCharsets.UTF_8);
        byte[] vendor = getVendor().getBytes(StandardCharsets.UTF_8);

        // Metadata count
        int metadataCount = 0;

        for (@NotNull Map.Entry<String, String> entry : metadata.entrySet()) {
            byte[] key = entry.getKey().getBytes(StandardCharsets.UTF_8);
            byte[] value = entry.getValue().getBytes(StandardCharsets.UTF_8);

            metadataCount += 4;
            metadataCount += key.length;
            metadataCount += value.length;
        }

        // Generate buffer
        @NotNull ByteBuffer buffer = ByteBuffer.allocate(1 + type.length + 2 + getToken().length + 1 + metadataCount + 1 + version.length + 1 + vendor.length);

        // Write it all
        buffer.put((byte) type.length);
        buffer.put(type);
        buffer.putShort((short) token.length);
        buffer.put(getToken());
        buffer.put((byte) metadata.size());

        for (@NotNull Map.Entry<String, String> entry : metadata.entrySet()) {
            byte[] key = entry.getKey().getBytes(StandardCharsets.UTF_8);
            byte[] value = entry.getValue().getBytes(StandardCharsets.UTF_8);

            buffer.putShort((short) key.length);
            buffer.put(key);

            buffer.putShort((short) value.length);
            buffer.put(value);
        }

        buffer.put((byte) version.length);
        buffer.put(version);

        buffer.put((byte) vendor.length);
        buffer.put(vendor);

        // Finish
        buffer.flip();
        return buffer;
    }
}