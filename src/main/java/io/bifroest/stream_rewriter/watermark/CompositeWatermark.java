/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.bifroest.stream_rewriter.watermark;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author hkraemer@ggs-hh.net
 */
public class CompositeWatermark implements Watermark {
    private final List<Watermark> watermarks = new ArrayList<>();
    
    public synchronized void addWatermark( Watermark newMark ) {
        watermarks.add( newMark );
    }
    @Override
    public synchronized boolean overHigh() {
        return watermarks.stream().anyMatch( wm -> wm.overHigh() );
    }

    @Override
    public synchronized boolean belowLow() {
        return watermarks.stream().allMatch( wm -> wm.belowLow() );
    }
    
}
