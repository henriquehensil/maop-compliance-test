package dev.hensil.maop.compliance.client.situation.authentication;

import dev.hensil.maop.compliance.client.Main;
import dev.hensil.maop.compliance.client.connection.BidirectionalStream;
import dev.hensil.maop.compliance.client.connection.Connection;
import dev.hensil.maop.compliance.client.connection.Connections;
import dev.hensil.maop.compliance.client.exception.ConnectionException;
import dev.hensil.maop.compliance.client.exception.InvalidVersionException;
import dev.hensil.maop.compliance.client.exception.PoolException;
import dev.hensil.maop.compliance.client.situation.Situation;
import dev.meinicke.plugin.annotation.*;
import dev.meinicke.semver.Version;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Estabelece uma conexão e se authentica em Servidor MAOP.
 * <br><br>
 *
 * <p>Possíveis erros
 *
 * <li>
 *     <b>ConnectionException</b>: Exceção ocorrida no primeiro momento em que se tenta estabelecer uma conexão com o servidor
 * <p>Provavelmente, esse erro aparecerá em nível de protocolo, mais precisamente o protocolo QUIC; seja pela
 * authenticação incial onde contratos entre servidor e cliente estão mal informados ou
 * alguma instabiliade no fluxo de conexões.
 * <p>Desenvolvedores devem se atentar nos detalhes de protocolo, como versão TLS, indentificador e versões
 * usadas. Assim como é descritas na <a href="https://mao-protocol.wiki/">página oficial</a>.
 * </li>
 * */

@Plugin
@Category("Situation")
@Priority(value = 1)
public final class NormalAuthenticationSituation implements Situation {

    @Override
    public @NotNull String getName() {
        return "Normal Authentication";
    }

    public boolean execute() {
        try {
            Main.log.info("Initiating connection to MAOP server.");
            @NotNull Connection connection = Connections.getInstance().create("normal");
            Main.log.info("Successfully connected");

            Main.log.info("Creating a new bidirectional pool to start authentication.");
            @NotNull BidirectionalStream stream = connection.getPools().createBidirectional();
            Main.log.info("Successfully created");

            Main.log.info("Initiating the MAOP authentication protocol");

            int timeout = 3;
            @NotNull TimeUnit unit = TimeUnit.SECONDS;
            {
                // Variables
                byte[] type = Main.getType().getBytes(StandardCharsets.UTF_8);
                byte[] token = Main.getToken().getBytes();
                byte[] version = Main.getVersion().toString().getBytes(StandardCharsets.UTF_8);
                byte[] vendor = Main.getVendor().getBytes(StandardCharsets.UTF_8);
                @NotNull Map<String, String> metadata = Main.getMetadata();

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
                @NotNull ByteBuffer buffer = ByteBuffer.allocate(1 + type.length + 2 + token.length + 1 + metadataCount + 1 + version.length + 1 + vendor.length);

                // Write it all
                buffer.put((byte) type.length);
                buffer.put(type);
                buffer.putShort((short) token.length);
                buffer.put(token);
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
                buffer.flip();

                Main.log.trace("Writing authentication to the server");

                {
                    @NotNull CompletableFuture<Void> future = new CompletableFuture<>();
                    future.orTimeout(timeout, unit);

                    CompletableFuture.runAsync(() -> {
                        try {
                            long ms = System.currentTimeMillis();
                            stream.write(buffer.array(), buffer.position(), buffer.limit());
                            int time = Math.toIntExact(ms - System.currentTimeMillis());

                            if (time > 300) {
                                Main.log.warn("The connection took " + time + " milliseconds to write the response slowly!");
                            } else {
                                Main.log.warn("The connection took " + time + " milliseconds to write the response");
                            }

                            future.complete(null);
                        } catch (IOException e) {
                            future.completeExceptionally(e);
                        }
                    });

                    try {
                        future.join();
                        Main.log.trace("Successfully written authentication bytes");
                        Main.log.trace("Closing the output");
                        stream.closeOutput();
                    } catch (CompletionException e) {
                        if (e.getCause() instanceof IOException io) {
                            Main.log.severe("Error while attempting to write on stream connection: " + io.getMessage());
                            return false;
                        } else if (e.getCause() instanceof TimeoutException) {
                            Main.log.severe("The timeout stream to write has expired: " + e.getClass().getSimpleName());
                            Main.log.severe("The timeout was set as " + timeout + " " + unit + " to write and the timeout was reached");
                            return false;
                        }

                        throw new Exception("Internal error");
                    } catch (IOException ignore) {
                    }
                }
            }

            Main.log.trace("Start to read the server response");

            @NotNull CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
            future.orTimeout(timeout, unit);
            CompletableFuture.runAsync(() -> {
                try {
                    Main.log.trace("Prepare an buffer and cache");
                    @NotNull ByteArrayOutputStream array = new ByteArrayOutputStream();
                    byte[] cache = new byte[1024];

                    int read;
                    while ((read = stream.read(cache)) != -1) {
                        Main.log.debug("Read \"" + read + "\" bytes from the peer stream");
                        array.write(cache, 0, read);
                    }

                    Main.log.debug("Total read from the stream: " + array.size());

                    Main.log.trace("Closing the input");
                    stream.close();

                    future.complete(ByteBuffer.wrap(array.toByteArray()));

                    Main.log.info("Reading completed and buffered");
                } catch (IOException e) {
                    future.completeExceptionally(e);
                }
            });

            @NotNull ByteBuffer buffer;
            try {
                long ms = System.currentTimeMillis();
                buffer = future.join();
                int time = Math.toIntExact(ms - System.currentTimeMillis());

                if (buffer.capacity() < 11) {
                    Main.log.severe("It seems that there are not enough bytes to assemble a response");
                    return false;
                }

                if (time > 500 && time <= 1000) {
                    Main.log.warn("The connection took " + time + " milliseconds to read the response in a reasonable way");
                } else if (time > 1000) {
                    Main.log.warn("The connection took " + time + " milliseconds to read the response slowly!!");
                }
            } catch (CompletionException e) {
                if (e.getCause() instanceof IOException io) {
                    Main.log.severe("Error while attempting to read on stream connection: " + io.getMessage());
                    return false;
                } else if (e.getCause() instanceof TimeoutException) {
                    Main.log.severe("The timeout stream to read has expired: " + e.getClass().getSimpleName());
                    Main.log.severe("The timeout was set as " + timeout + " " + unit + " to read and the timeout was reached");
                    return false;
                }

                throw new Exception("Internal error");
            }

            Main.log.info("Starting response verifications");
            buffer.flip();

            {
                // [approved:1]
                int approvedByte = Byte.toUnsignedInt(buffer.get());
                boolean approved = (approvedByte == 1);

                @Nullable UUID sessionId = null;
                @Nullable String identifier = null;

                short code = 0;
                @Nullable String reason = null;

                @NotNull Version version;

                if (approved) {
                    // [session_id:16]
                    long most = 0L;
                    long least = 0L;

                    for (int i = 0; i < 8; i++) {
                        most = (most << 8) | Byte.toUnsignedLong(buffer.get());
                    }
                    for (int i = 0; i < 8; i++) {
                        least = (least << 8) | Byte.toUnsignedLong(buffer.get());
                    }

                    sessionId = new UUID(most, least);

                    // [ identifier_len:1 ]
                    int idLen = Byte.toUnsignedInt(buffer.get());
                    byte[] idBytes = new byte[idLen];
                    buffer.get(idBytes);
                    identifier = new String(idBytes, StandardCharsets.UTF_8);
                } else {
                    // [ error_code:2 ]
                    code = buffer.getShort();
                    // [ retry_after_ms:4 ]
                    buffer.getInt();

                    // [ sanitized_reason_len:2 ]
                    int reasonLen = Short.toUnsignedInt(buffer.getShort());
                    if (reasonLen > 0) {
                        byte[] reasonBytes = new byte[reasonLen];
                        buffer.get(reasonBytes);
                        reason = new String(reasonBytes, StandardCharsets.UTF_8);
                    }
                }

                // [ version_len:1 ]
                int versionLen = Byte.toUnsignedInt(buffer.get());
                byte[] versionBytes = new byte[versionLen];
                buffer.get(versionBytes);
                @NotNull String versionString = new String(versionBytes, StandardCharsets.UTF_8);
                version = Version.parse(versionString);

                if (!version.equals(Main.getVersion())) {
                    throw new InvalidVersionException("Version type different from what was defined: " + version);
                }

                // [ vendor_len:1 ]
                int vendorLen = Byte.toUnsignedInt(buffer.get());
                byte[] vendorBytes = new byte[vendorLen];
                buffer.get(vendorBytes);
                new String(vendorBytes, StandardCharsets.UTF_8);

                if (!approved) {
                    Main.log.severe("Unable to authenticate on the server: Not approved.");
                    // todo try again for the last time using retry_after.

                    Main.log.severe("Error code: " + code);
                    Main.log.severe("Reason: " + reason);

                    return false;
                }

                Main.log.info("Successfully authenticated!");
                Main.log.info("Session id: " + sessionId);
                Main.log.info("Identifier: " + identifier);
            }

            connection.setAuthenticated(true);

            return true;
        } catch (ConnectionException e) {
            Main.log.severe("Error while attempting to establish connection: " + e.getClass().getSimpleName());
            Main.log.severe(e.toString());
            return false;
        } catch (PoolException e) {
            Main.log.severe("Error while attempting to create a new pool: " + e.getClass().getSimpleName());
            Main.log.severe(e.toString());
            return false;
        } catch (BufferUnderflowException e) {
            Main.log.severe("Insufficient bytes while to trying the verify response: " + e);
            return false;
        } catch (InvalidVersionException e) {
            Main.log.severe("Invalid version: " + e.getClass().getSimpleName());
            Main.log.severe(e.getMessage());
            return false;
        } catch (Throwable e) {
            Main.log.severe("An unknown error occurs: " + e);
            return false;
        }
    }
}