/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.bifroest.stream_rewriter.watermark;

/**
 *
 * @author hkraemer@ggs-hh.net
 */
public interface EnvironmentWithMutableCompositeWaterMark extends EnvironmentWithWatermark {
    void addWatermark( Watermark wm );
}
