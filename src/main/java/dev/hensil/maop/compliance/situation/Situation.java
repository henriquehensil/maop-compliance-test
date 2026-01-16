package dev.hensil.maop.compliance.situation;

import dev.hensil.maop.compliance.core.Compliance;

import org.jetbrains.annotations.NotNull;

import java.util.Objects;

public abstract class Situation {

    // Object

    public @NotNull String getName() {
        return getClass().getSimpleName()
                .replaceAll("(?<!^)([A-Z])", " $1")
                .toLowerCase();
    }

    /**
     * @return True if the diagnostic situation is severe
     * */
    public abstract boolean diagnostic(@NotNull Compliance compliance);

    @Override
    public final @NotNull String toString() {
        return getName();
    }

    @Override
    public final boolean equals(Object o) {
        return o != null && getClass() == o.getClass();
    }

    @Override
    public final int hashCode() {
        return Objects.hashCode(this.getClass());
    }
}