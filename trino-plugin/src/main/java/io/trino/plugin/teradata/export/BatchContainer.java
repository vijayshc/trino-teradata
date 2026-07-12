package io.trino.plugin.teradata.export;

import io.trino.spi.Page;

public record BatchContainer(Page page, boolean isEndOfStream) {
    public static BatchContainer endOfStream() {
        return new BatchContainer(null, true);
    }
    
    public static BatchContainer of(Page page) {
        return new BatchContainer(page, false);
    }
}
