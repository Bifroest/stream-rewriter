package io.bifroest.stream_rewriter.watermark;

import io.bifroest.commons.boot.interfaces.Environment;

public interface EnvironmentWithWatermark extends Environment {
    public Watermark watermark();
}
