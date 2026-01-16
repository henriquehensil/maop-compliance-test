package dev.hensil.maop.compliance;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public final class Elapsed {

    private final @NotNull Instant start;
    private @Nullable Instant end;

    public Elapsed() {
        this.start = Instant.now();
    }
    public Elapsed(@NotNull Instant start) {
        this.start = start;
    }

    // Getters

    public @NotNull Instant getStart() {
        return start;
    }
    public long getElapsedMillis() {
        if (end != null) {
            return end.toEpochMilli() - getStart().toEpochMilli();
        } else {
            return System.currentTimeMillis() - getStart().toEpochMilli();
        }
    }

    public void freeze() {
        this.end = Instant.now();
    }
    public void unfreeze() {
        this.end = null;
    }

    // Implementations

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        Elapsed elapsed = (Elapsed) object;
        return Objects.equals(getStart(), elapsed.getStart());
    }
    @Override
    public int hashCode() {
        return Objects.hashCode(getStart());
    }

    @Override
    public @NotNull String toString() {
        // Variables
        long millis = getElapsedMillis();

        long dd = TimeUnit.MILLISECONDS.toDays(millis);
        long hh = TimeUnit.MILLISECONDS.toHours(millis) % 24;
        long mm = TimeUnit.MILLISECONDS.toMinutes(millis) % 60;
        long ss = TimeUnit.MILLISECONDS.toSeconds(millis) % 60;
        long mmm = millis % 1000;

        // Directly return ms if (millis < 1000)
        if (millis < 1000) {
            return (millis + "ms");
        }

        // Build string
        @NotNull StringBuilder builder = new StringBuilder();
        if (dd != 0) builder.append(dd).append("d");
        if (hh != 0) builder.append(hh).append("h");
        if (mm != 0) builder.append(mm).append("m");
        if (ss != 0) builder.append(ss).append("s");
        if (mmm != 0) builder.append(mmm).append("ms");

        // Finish
        return builder.toString();
    }

}