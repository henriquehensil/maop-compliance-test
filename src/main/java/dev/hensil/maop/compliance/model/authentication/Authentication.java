package dev.hensil.maop.compliance.model.authentication;

import dev.hensil.maop.compliance.core.Preset;

import dev.hensil.maop.compliance.model.Version;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Unmodifiable;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

public final class Authentication {

    // Static initializer

    @NotNull
    public static Authentication parse(@NotNull ByteBuffer buffer) {
        int typeLen = Byte.toUnsignedInt(buffer.get());
        byte[] typeBytes = new byte[typeLen];
        buffer.get(typeBytes);
        String type = new String(typeBytes, StandardCharsets.UTF_8);
        int tokenLen = Short.toUnsignedInt(buffer.getShort());
        byte[] token = new byte[tokenLen];
        buffer.get(token);
        int pairs = Byte.toUnsignedInt(buffer.get());
        Map<String, String> metadata = new LinkedHashMap<>(pairs);

        for(int i = 0; i < pairs; ++i) {
            int keyLen = Short.toUnsignedInt(buffer.getShort());
            byte[] keyBytes = new byte[keyLen];
            buffer.get(keyBytes);
            String key = new String(keyBytes, StandardCharsets.UTF_8);
            int valueLen = Short.toUnsignedInt(buffer.getShort());
            byte[] valueBytes = new byte[valueLen];
            buffer.get(valueBytes);
            String value = new String(valueBytes, StandardCharsets.UTF_8);
            metadata.put(key, value);
        }

        int versionLen = Byte.toUnsignedInt(buffer.get());
        byte[] versionBytes = new byte[versionLen];
        buffer.get(versionBytes);
        String versionString = new String(versionBytes, StandardCharsets.UTF_8);
        Version version = Version.parse(versionString);
        int vendorLen = Byte.toUnsignedInt(buffer.get());
        byte[] vendorBytes = new byte[vendorLen];
        buffer.get(vendorBytes);
        String vendor = new String(vendorBytes, StandardCharsets.UTF_8);
        return new Authentication(type, token, metadata, version, vendor);
    }

    // Objects

    private final @NotNull String type;
    private final byte[] token;
    private final @NotNull Map<String, String> metadata;
    private final @NotNull String version;
    private final @NotNull String vendor;

    public Authentication(@NotNull Preset preset) {
        this(preset.getAuthenticationType(), preset.getAuthenticationToken(), preset.getAuthenticationMetadata(), preset.getVersion(), preset.getVendor());
    }

    public Authentication(@NotNull String type, byte[] token, @NotNull Map<String, String> metadata, @NotNull String version, @NotNull String vendor) {
        this.type = type;
        this.token = token;
        this.metadata = Map.copyOf(metadata);
        this.version = version;
        this.vendor = vendor;
        if (type.getBytes(StandardCharsets.UTF_8).length > 255) {
            throw new IllegalArgumentException("the type length cannot be higher than 255 characters.");
        } else if (token.length > 65535) {
            throw new IllegalArgumentException("the token length cannot be higher than 65535 bytes.");
        } else if (metadata.size() > 255) {
            throw new IllegalArgumentException("the metadata size cannot be higher than 255.");
        } else if (metadata.keySet().stream().anyMatch((key) -> key.getBytes(StandardCharsets.UTF_8).length > 65535)) {
            throw new IllegalArgumentException("the metadata key length cannot be higher than 65535.");
        } else if (metadata.values().stream().anyMatch((value) -> value.getBytes(StandardCharsets.UTF_8).length > 65535)) {
            throw new IllegalArgumentException("the metadata value length cannot be higher than 65535.");
        } else if (version.getBytes(StandardCharsets.UTF_8).length > 255) {
            throw new IllegalArgumentException("the version length cannot be higher than 255.");
        } else if (vendor.getBytes(StandardCharsets.UTF_8).length > 255) {
            throw new IllegalArgumentException("the vendor length cannot be higher than 255.");
        }
    }

    public Authentication(@NotNull String type, byte[] token, @NotNull Map<String, String> metadata, @NotNull Version version, @NotNull String vendor) {
        this(type, token, metadata, version.toString(), vendor);
    }

    public @NotNull String getType() {
        return this.type;
    }

    public byte[] getToken() {
        return this.token;
    }

    public @Unmodifiable @NotNull Map<String, String> getMetadata() {
        return this.metadata;
    }

    public @NotNull String getVersion() {
        return this.version;
    }

    public @NotNull String getVendor() {
        return this.vendor;
    }

    @NotNull
    public ByteBuffer toByteBuffer() {
        byte[] type = this.getType().getBytes(StandardCharsets.UTF_8);
        byte[] version = this.getVersion().getBytes(StandardCharsets.UTF_8);
        byte[] vendor = this.getVendor().getBytes(StandardCharsets.UTF_8);
        int metadataCount = 0;

        for(Map.Entry<String, String> entry : this.metadata.entrySet()) {
            byte[] key = entry.getKey().getBytes(StandardCharsets.UTF_8);
            byte[] value = entry.getValue().getBytes(StandardCharsets.UTF_8);
            metadataCount += 4;
            metadataCount += key.length;
            metadataCount += value.length;
        }

        ByteBuffer buffer = ByteBuffer.allocate(1 + type.length + 2 + this.getToken().length + 1 + metadataCount + 1 + version.length + 1 + vendor.length);
        buffer.put((byte)type.length);
        buffer.put(type);
        buffer.putShort((short)this.token.length);
        buffer.put(this.getToken());
        buffer.put((byte)this.metadata.size());

        for(Map.Entry<String, String> entry : this.metadata.entrySet()) {
            byte[] key = entry.getKey().getBytes(StandardCharsets.UTF_8);
            byte[] value = entry.getValue().getBytes(StandardCharsets.UTF_8);
            buffer.putShort((short)key.length);
            buffer.put(key);
            buffer.putShort((short)value.length);
            buffer.put(value);
        }

        buffer.put((byte)version.length);
        buffer.put(version);
        buffer.put((byte)vendor.length);
        buffer.put(vendor);
        buffer.flip();
        return buffer;
    }
}