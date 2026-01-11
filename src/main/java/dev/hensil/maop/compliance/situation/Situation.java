package dev.hensil.maop.compliance.situation;

import dev.hensil.maop.compliance.core.Compliance;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Objects;

public abstract class Situation {

    // Object

    private @Nullable Compliance compliance;

    public @NotNull String getName() {
        return getClass().getSimpleName()
                .replaceAll("(?<!^)([A-Z])", " $1")
                .toLowerCase();
    }

    public final @NotNull Compliance getCompliance() {
        if (compliance == null) {
            throw new AssertionError("Internal error");
        }

        return compliance;
    }

    /**
     * @return True if the diagnostic situation is severe
     * */
    public abstract boolean diagnostic();

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