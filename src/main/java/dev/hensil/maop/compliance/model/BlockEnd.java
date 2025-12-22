package dev.hensil.maop.compliance.model;

public final class BlockEnd extends Operation {

    private final long total;

    public BlockEnd(long total) {
        super((byte) 0x06);
        this.total = total;
    }

    public long getTotal() {
        return total;
    }
}
