/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.bifroest.stream_rewriter.db;

import io.bifroest.commons.model.Metric;
import java.util.concurrent.BlockingQueue;

/**
 *
 * @author hkraemer@ggs-hh.net
 */
public interface EnvironmentWithMutableDBInput extends EnvironmentWithDBInput {
    void setDbInputQueue( BlockingQueue<Metric> queue );
}
