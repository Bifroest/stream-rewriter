package io.bifroest.stream_rewriter.db;

import java.util.concurrent.BlockingQueue;

import io.bifroest.commons.boot.interfaces.Environment;
import io.bifroest.commons.model.Metric;

/**
 *
 * @author hkraemer@ggs-hh.net
 */
public interface EnvironmentWithDBInput extends Environment {
    BlockingQueue<Metric> dbInputQueue();
}
