package com.goodgame.profiling.stream_rewriter.watermark;

import com.goodgame.profiling.commons.boot.interfaces.Environment;

public interface EnvironmentWithWatermark extends Environment {
    public Watermark watermark();
}
