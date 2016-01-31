package io.bifroest.stream_rewriter.netty;

public interface EnvironmentWithMutableNettyPortController extends EnvironmentWithNettyPortController {
    void setNettyPortController( NettyPortController nettyPortController );
}
