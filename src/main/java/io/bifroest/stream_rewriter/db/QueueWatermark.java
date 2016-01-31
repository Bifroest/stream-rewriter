/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.bifroest.stream_rewriter.db;

import io.bifroest.stream_rewriter.watermark.Watermark;

import java.util.concurrent.BlockingQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author hkraemer@ggs-hh.net
 */
public class QueueWatermark implements Watermark {
    private static final Logger log = LogManager.getLogger();

    private final double highFraction;
    private final double lowFraction;
    private final BlockingQueue<?> queue;
    private final int queueCapacity;

    public QueueWatermark( double lowFraction, double highFraction, BlockingQueue<?> queue, int queueCapacity ) {
        this.lowFraction = lowFraction;
        this.highFraction = highFraction;
        this.queue = queue;
        this.queueCapacity = queueCapacity;
    }

    private double queueFillFraction() {
        int elementsInQueue = queue.size();
        return elementsInQueue / (double) queueCapacity;
    }

    @Override
    public boolean overHigh() {
        if ( log.isTraceEnabled() ) {
            log.trace( String.format( "Current queue Fill Percentage: %.2f", queueFillFraction() * 100 ) );
        }
        return queueFillFraction() > highFraction;
    }

    @Override
    public boolean belowLow() {
        if ( log.isTraceEnabled() ) {
            log.trace( String.format( "Current queue Fill Percentage: %.2f", queueFillFraction() * 100 ) );
        }
        return queueFillFraction() < lowFraction;
    }
}
