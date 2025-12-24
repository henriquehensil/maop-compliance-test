package dev.hensil.maop.compliance.situation;

import dev.hensil.maop.compliance.Compliance;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Objects;

public abstract class Situation {

    private final @NotNull String name;
    private @Nullable Compliance compliance;

    protected Situation() {
        this.name = getClass().getSimpleName()
                .replaceAll("(?<!^)([A-Z])", " $1")
                .toLowerCase();
    }

    protected Situation(@NotNull String name) {
        name = name.toLowerCase().endsWith("situation") ? name : name + " situation";
        this.name = name.toLowerCase();
    }

    public final @NotNull String getName() {
        return name;
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
        return name;
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