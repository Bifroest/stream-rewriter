package io.bifroest.stream_rewriter.watermark;

/**
 *
 * @author hkraemer@ggs-hh.net
 */
public interface Watermark {
    public boolean overHigh();
    public boolean belowLow();
}
