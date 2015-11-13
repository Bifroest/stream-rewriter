package com.goodgame.profiling.stream_rewriter.netty;

import com.goodgame.profiling.commons.boot.interfaces.Environment;

public interface EnvironmentWithNettyPortController extends Environment {
    NettyPortController nettyPortController();
}
