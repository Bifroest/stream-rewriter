package io.bifroest.stream_rewriter.netty;

public interface NettyPortController {
    public void openNettyPort() throws InterruptedException;
    public void closeNettyPort() throws InterruptedException;
    public boolean isPortOpen();
}
