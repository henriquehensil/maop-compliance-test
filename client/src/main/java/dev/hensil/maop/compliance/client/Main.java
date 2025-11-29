package dev.hensil.maop.compliance.client;

import codes.laivy.address.host.Host;
import com.jlogm.Logger;
import dev.hensil.maop.compliance.client.protocol.authentication.Authentication;
import dev.hensil.maop.compliance.client.situation.Situation;
import dev.meinicke.plugin.exception.PluginInitializeException;
import dev.meinicke.plugin.main.Plugins;
import dev.meinicke.semver.Version;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.*;

public final class Main {

    // Static initializers

    public static final @NotNull Logger log = Logger.create("MAOP-Client");

    private static @Nullable Host host;
    private static @Nullable String token;
    private static @Nullable Map<String, String> metadata;
    private static @Nullable Version version;
    private static @Nullable String vendor;
    private static @Nullable String type;

    public static void main(String @NotNull [] args) throws PluginInitializeException, IOException {
        @NotNull Scanner scanner = new Scanner(System.in);
        scanner.useLocale(Locale.US);

        if (host == null) {
            System.out.print("Choose the server host: ");
            @NotNull String str = scanner.nextLine();
            host = Host.parse(str);
        }

        if (type == null) {
            System.out.print("Enter the server authentication type: ");

        }

        if (token == null) {
            System.out.print("Enter the server token: ");
            @NotNull String str = scanner.nextLine();
            token = str;
        }

        if (metadata == null) {
            metadata = new LinkedHashMap<>();
        }

        if (version == null) {
            version = Version.parse("1.0.0");
        }

        if (vendor == null) {
            System.out.print("Enter the server vendor: ");
            @NotNull String str = scanner.nextLine();
            vendor = str;
        }

        // Load all situations
        @NotNull Set<@NotNull Situation> situations = new LinkedHashSet<>();
    }

    public static @NotNull Host getHost() {
        return Objects.requireNonNull(host, "Not defined");
    }

    public static @NotNull String getType() {
        return Objects.requireNonNull(type, "Not defined");
    }

    public static @NotNull String getToken() {
        return Objects.requireNonNull(token, "Not defined");
    }

    public static @NotNull Map<String, String> getMetadata() {
        return Objects.requireNonNull(metadata, "Not defined");
    }

    public static @NotNull Version getVersion() {
        return Objects.requireNonNull(version, "Not defined");
    }

    public static @NotNull String getVendor() {
        return Objects.requireNonNull(vendor, "Not defined");
    }

    public static @NotNull Authentication newAuthentication() {
        return new Authentication(getType(), getToken().getBytes(), getMetadata(), getVersion(), getVendor());
    }

    // Objects

    private Main() {
        throw new UnsupportedOperationException();
    }
}