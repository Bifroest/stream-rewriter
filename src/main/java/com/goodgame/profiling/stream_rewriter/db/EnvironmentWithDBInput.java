package com.goodgame.profiling.stream_rewriter.db;

import java.util.concurrent.BlockingQueue;

import com.goodgame.profiling.commons.boot.interfaces.Environment;
import com.goodgame.profiling.commons.model.Metric;

/**
 *
 * @author hkraemer@ggs-hh.net
 */
public interface EnvironmentWithDBInput extends Environment {
    BlockingQueue<Metric> dbInputQueue();
}
