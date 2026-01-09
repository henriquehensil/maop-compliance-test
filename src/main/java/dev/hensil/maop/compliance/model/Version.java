package dev.hensil.maop.compliance.model;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigInteger;
import java.util.*;

public final class Version implements Comparable<Version> {

    // Static initializers

    public static final @NotNull Comparator<Version> PRECEDENCE = Version::compareTo;

    public static final @NotNull Comparator<Version> PRECEDENCE_WITH_BUILD = (a, b) -> {
        int p = a.compareTo(b);
        if (p != 0) {
            return p;
        } else {
            List<String> ab = a.build;
            List<String> bb = b.build;
            int min = Math.min(ab.size(), bb.size());

            for(int i = 0; i < min; ++i) {
                String as = ab.get(i);
                String bs = bb.get(i);
                boolean an = Version.Parser.isNumeric(as);
                boolean bn = Version.Parser.isNumeric(bs);
                if (an && bn) {
                    BigInteger A = new BigInteger(as);
                    BigInteger B = new BigInteger(bs);
                    int cmp = A.compareTo(B);
                    if (cmp != 0) {
                        return cmp;
                    }
                } else {
                    int cmp = as.compareTo(bs);
                    if (cmp != 0) {
                        return cmp;
                    }
                }
            }

            return Integer.compare(ab.size(), bb.size());
        }
    };

    public static @NotNull Version parse(@NotNull String input) {
        return (new Version.Parser(input)).parse();
    }

    public static @NotNull Optional<Version> tryParse(@Nullable String input) {
        if (input == null) {
            return Optional.empty();
        } else {
            try {
                return Optional.of(parse(input));
            } catch (IllegalArgumentException var2) {
                return Optional.empty();
            }
        }
    }

    public static @NotNull Version of(@NotNull BigInteger major, @NotNull BigInteger minor, @NotNull BigInteger patch, @Nullable List<String> prereleaseList, @Nullable List<String> buildList) {
        Objects.requireNonNull(major, "major");
        Objects.requireNonNull(minor, "minor");
        Objects.requireNonNull(patch, "patch");
        if (major.signum() >= 0 && minor.signum() >= 0 && patch.signum() >= 0) {
            List<String> pr = prereleaseList == null ? Collections.emptyList() : new ArrayList<>(prereleaseList);
            List<String> bd = buildList == null ? Collections.emptyList() : new ArrayList<>(buildList);

            for(String id : pr) {
                Version.Parser.validatePrereleaseIdentifier(id);
            }

            for(String id : bd) {
                Version.Parser.validateBuildIdentifier(id);
            }

            return new Version(major, minor, patch, pr, bd);
        } else {
            throw new IllegalArgumentException("major/minor/patch must be non-negative");
        }
    }

    // Objects

    private final @NotNull BigInteger major;
    private final @NotNull BigInteger minor;
    private final @NotNull BigInteger patch;
    private final List<String> prerelease;
    private final List<String> build;
    private final @NotNull String canonical;

    private Version(@NotNull BigInteger major, @NotNull BigInteger minor, @NotNull BigInteger patch, @NotNull List<String> prerelease, @NotNull List<String> build) {
        this.major = Objects.requireNonNull(major, "major");
        this.minor = Objects.requireNonNull(minor, "minor");
        this.patch = Objects.requireNonNull(patch, "patch");
        this.prerelease = List.copyOf(Objects.requireNonNull(prerelease, "prerelease"));
        this.build = List.copyOf(Objects.requireNonNull(build, "build"));
        this.canonical = this.buildCanonical();
    }

    public @NotNull BigInteger getMajor() {
        return this.major;
    }

    public @NotNull BigInteger getMinor() {
        return this.minor;
    }

    public @NotNull BigInteger getPatch() {
        return this.patch;
    }

    public @NotNull List<String> getPrerelease() {
        return this.prerelease;
    }

    public @NotNull List<String> getBuild() {
        return this.build;
    }

    public boolean isPrerelease() {
        return !this.prerelease.isEmpty();
    }

    public @NotNull Version bumpMajor() {
        return new Version(this.major.add(BigInteger.ONE), BigInteger.ZERO, BigInteger.ZERO, Collections.emptyList(), Collections.emptyList());
    }

    public @NotNull Version bumpMinor() {
        return new Version(this.major, this.minor.add(BigInteger.ONE), BigInteger.ZERO, Collections.emptyList(), Collections.emptyList());
    }

    public @NotNull Version bumpPatch() {
        return new Version(this.major, this.minor, this.patch.add(BigInteger.ONE), Collections.emptyList(), Collections.emptyList());
    }

    public @NotNull String toString() {
        return this.canonical;
    }

    private @NotNull String buildCanonical() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.major).append('.').append(this.minor).append('.').append(this.patch);
        if (!this.prerelease.isEmpty()) {
            sb.append('-');
            joinDot(sb, this.prerelease);
        }

        if (!this.build.isEmpty()) {
            sb.append('+');
            joinDot(sb, this.build);
        }

        return sb.toString();
    }

    private static void joinDot(@NotNull StringBuilder sb, @NotNull List<String> list) {
        for(int i = 0; i < list.size(); ++i) {
            if (i > 0) {
                sb.append('.');
            }

            sb.append(list.get(i));
        }
    }

    public int compareTo(@NotNull Version other) {
        Objects.requireNonNull(other, "other");
        int cmp = this.major.compareTo(other.major);
        if (cmp != 0) {
            return cmp;
        } else {
            cmp = this.minor.compareTo(other.minor);
            if (cmp != 0) {
                return cmp;
            } else {
                cmp = this.patch.compareTo(other.patch);
                if (cmp != 0) {
                    return cmp;
                } else {
                    boolean aPre = !this.prerelease.isEmpty();
                    boolean bPre = !other.prerelease.isEmpty();
                    if (!aPre && !bPre) {
                        return 0;
                    } else if (!aPre) {
                        return 1;
                    } else if (!bPre) {
                        return -1;
                    } else {
                        int min = Math.min(this.prerelease.size(), other.prerelease.size());

                        for(int i = 0; i < min; ++i) {
                            String aId = this.prerelease.get(i);
                            String bId = other.prerelease.get(i);
                            boolean aNum = Version.Parser.isNumeric(aId);
                            boolean bNum = Version.Parser.isNumeric(bId);
                            if (aNum && bNum) {
                                BigInteger A = new BigInteger(aId);
                                BigInteger B = new BigInteger(bId);
                                int n = A.compareTo(B);
                                if (n != 0) {
                                    return n;
                                }
                            } else {
                                if (aNum) {
                                    return -1;
                                }

                                if (bNum) {
                                    return 1;
                                }

                                int n = aId.compareTo(bId);
                                if (n != 0) {
                                    return n;
                                }
                            }
                        }

                        return Integer.compare(this.prerelease.size(), other.prerelease.size());
                    }
                }
            }
        }
    }

    public boolean equals(@Nullable Object o) {
        if (this == o) {
            return true;
        } else if (!(o instanceof Version v)) {
            return false;
        } else {
            return this.major.equals(v.major) && this.minor.equals(v.minor) && this.patch.equals(v.patch) && this.prerelease.equals(v.prerelease) && this.build.equals(v.build);
        }
    }

    public int hashCode() {
        return Objects.hash(this.major, this.minor, this.patch, this.prerelease, this.build);
    }

    private static final class Parser {
        private final @NotNull String input;

        Parser(@NotNull String input) {
            this.input = Objects.requireNonNull(input, "input").trim();
            if (this.input.isEmpty()) {
                throw new IllegalArgumentException("Empty version string");
            }
        }

        Version parse() {
            String s = this.input;
            int plus = s.indexOf(43);
            String buildPart = plus >= 0 ? s.substring(plus + 1) : "";
            String beforePlus = plus >= 0 ? s.substring(0, plus) : s;
            int dash = beforePlus.indexOf(45);
            String core = dash >= 0 ? beforePlus.substring(0, dash) : beforePlus;
            String prereleasePart = dash >= 0 ? beforePlus.substring(dash + 1) : "";
            if (dash >= 0 && prereleasePart.isEmpty()) {
                throw new IllegalArgumentException("Empty prerelease part after '-' in version: '" + this.input + "'");
            } else if (plus >= 0 && buildPart.isEmpty()) {
                throw new IllegalArgumentException("Empty build metadata part after '+' in version: '" + this.input + "'");
            } else {
                String[] corePieces = splitExact(core, '.');
                if (corePieces.length != 3) {
                    throw new IllegalArgumentException("Core version must be three dot-separated numeric identifiers: 'major.minor.patch' (found: '" + core + "')");
                } else {
                    BigInteger major = parseCoreNumber(corePieces[0], "major");
                    BigInteger minor = parseCoreNumber(corePieces[1], "minor");
                    BigInteger patch = parseCoreNumber(corePieces[2], "patch");
                    List<String> pre = prereleasePart.isEmpty() ? Collections.emptyList() : splitAndValidateIdentifiers(prereleasePart, true);
                    List<String> bd = buildPart.isEmpty() ? Collections.emptyList() : splitAndValidateIdentifiers(buildPart, false);
                    return new Version(major, minor, patch, pre, bd);
                }
            }
        }

        private static @NotNull String[] splitExact(@NotNull String s, char delim) {
            if (s.isEmpty()) {
                return new String[0];
            } else {
                List<String> parts = new ArrayList<>();
                int start = 0;

                for(int i = 0; i < s.length(); ++i) {
                    if (s.charAt(i) == delim) {
                        parts.add(s.substring(start, i));
                        start = i + 1;
                    }
                }

                parts.add(s.substring(start));
                return parts.toArray(new String[0]);
            }
        }

        private static BigInteger parseCoreNumber(String piece, String name) {
            if (piece != null && !piece.isEmpty()) {
                for(int i = 0; i < piece.length(); ++i) {
                    char c = piece.charAt(i);
                    if (c < '0' || c > '9') {
                        throw new IllegalArgumentException(name + " component must be numeric: '" + piece + "'");
                    }
                }

                if (piece.length() > 1 && piece.charAt(0) == '0') {
                    throw new IllegalArgumentException(name + " component must not contain leading zeros: '" + piece + "'");
                } else {
                    return new BigInteger(piece);
                }
            } else {
                throw new IllegalArgumentException(name + " component is missing");
            }
        }

        private static List<String> splitAndValidateIdentifiers(String s, boolean isPrerelease) {
            if (s.isEmpty()) {
                return Collections.emptyList();
            } else {
                String[] parts = splitExact(s, '.');
                List<String> out = new ArrayList<>(parts.length);

                for(String p : parts) {
                    if (p.isEmpty()) {
                        throw new IllegalArgumentException("Empty identifier in " + (isPrerelease ? "prerelease" : "build") + " part");
                    }

                    if (isPrerelease) {
                        validatePrereleaseIdentifier(p);
                    } else {
                        validateBuildIdentifier(p);
                    }

                    out.add(p);
                }

                return Collections.unmodifiableList(out);
            }
        }

        private static void validatePrereleaseIdentifier(String id) {
            if (id != null && !id.isEmpty()) {
                if (!isAsciiIdentifier(id)) {
                    throw new IllegalArgumentException("Prerelease identifier contains invalid characters: '" + id + "'");
                } else if (isNumeric(id) && id.length() > 1 && id.charAt(0) == '0') {
                    throw new IllegalArgumentException("Numeric prerelease identifier must not contain leading zeros: '" + id + "'");
                }
            } else {
                throw new IllegalArgumentException("Empty prerelease identifier");
            }
        }

        private static void validateBuildIdentifier(String id) {
            if (id != null && !id.isEmpty()) {
                if (!isAsciiIdentifier(id)) {
                    throw new IllegalArgumentException("Build identifier contains invalid characters: '" + id + "'");
                }
            } else {
                throw new IllegalArgumentException("Empty build identifier");
            }
        }

        private static boolean isAsciiIdentifier(@NotNull String id) {
            for(int i = 0; i < id.length(); ++i) {
                char c = id.charAt(i);
                if ((c < '0' || c > '9') && (c < 'A' || c > 'Z') && (c < 'a' || c > 'z') && c != '-') {
                    return false;
                }
            }

            return true;
        }

        private static boolean isNumeric(@Nullable String s) {
            if (s != null && !s.isEmpty()) {
                for(int i = 0; i < s.length(); ++i) {
                    char c = s.charAt(i);
                    if (c < '0' || c > '9') {
                        return false;
                    }
                }

                return true;
            } else {
                return false;
            }
        }
    }
}