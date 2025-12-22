package dev.hensil.maop.compliance.situation;

import org.jetbrains.annotations.NotNull;

public interface Situation {

    @NotNull String getName();

    /**
     * @return True if the diagnostic situation is severe
     * */
    boolean diagnostic();

}