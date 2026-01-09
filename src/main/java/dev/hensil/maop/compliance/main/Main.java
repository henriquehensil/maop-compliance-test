package dev.hensil.maop.compliance.main;

import codes.laivy.address.Address;
import codes.laivy.address.port.Port;
import com.jlogm.Logger;
import com.jlogm.formatter.DefaultFormatter;
import dev.hensil.maop.compliance.BidirectionalStream;
import dev.hensil.maop.compliance.Compliance;
import dev.hensil.maop.compliance.Connection;
import dev.hensil.maop.compliance.Preset;
import dev.hensil.maop.compliance.situation.Situation;
import org.jetbrains.annotations.NotNull;
import wiki.maop.server.MaopServer;

import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.security.KeyStore;
import java.util.Arrays;

public final class Main {

    public static void main(String[] args) throws Throwable {
//        @NotNull KeyStore keyStore = KeyStore.getInstance("JKS");
//        try (@NotNull InputStream is = new FileInputStream("C:\\Users\\User\\server.jks")) {
//            keyStore.load(is, "123456".toCharArray());
//        }
//
//        @NotNull Address address = Address.parse("localhost");
//        @NotNull Port port = Port.create(80);
//
//        @NotNull MaopServer server = new MaopServer(keyStore, new KeyStore.PasswordProtection("123456".toCharArray()), address, port);
//        server.open();
//
//        // Start compliance
//        @NotNull Preset preset = Preset.newBuilder()
//                .uri(URI.create("https://localhost:80"))
//                .authenticationType("Basic")
//                .vendor("Tester")
//                .build();
//
//        @NotNull Compliance compliance = new Compliance(preset);
//        compliance.start();
    }
}