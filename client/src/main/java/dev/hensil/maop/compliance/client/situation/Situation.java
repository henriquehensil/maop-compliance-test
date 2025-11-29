package dev.hensil.maop.compliance.client.situation;

import org.jetbrains.annotations.NotNull;

public interface Situation {

    @NotNull String getName();

    boolean execute();

}