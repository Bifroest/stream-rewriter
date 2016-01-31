package io.bifroest.stream_rewriter.netty;

import io.bifroest.commons.boot.interfaces.Environment;

public interface EnvironmentWithNettyPortController extends Environment {
    NettyPortController nettyPortController();
}
