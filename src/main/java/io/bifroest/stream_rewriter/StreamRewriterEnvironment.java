package io.bifroest.stream_rewriter;

import java.nio.file.Path;
import java.util.concurrent.BlockingQueue;

import io.bifroest.bifroest_client.BifroestClient;
import io.bifroest.bifroest_client.EnvironmentWithMutableBifroestClient;
import io.bifroest.commons.boot.InitD;
import io.bifroest.commons.boot.interfaces.Environment;
import io.bifroest.commons.environment.AbstractCommonEnvironment;
import io.bifroest.commons.model.Metric;
import io.bifroest.retentions.RetentionConfiguration;
import io.bifroest.retentions.bootloader.EnvironmentWithMutableRetentionStrategy;
import io.bifroest.stream_rewriter.db.EnvironmentWithMutableDBInput;
import io.bifroest.stream_rewriter.netty.EnvironmentWithMutableNettyPortController;
import io.bifroest.stream_rewriter.netty.NettyPortController;
import io.bifroest.stream_rewriter.persistent_drains.EnvironmentWithMutablePersistentDrainManager;
import io.bifroest.stream_rewriter.persistent_drains.PersistentDrainManager;
import io.bifroest.stream_rewriter.watermark.CompositeWatermark;
import io.bifroest.stream_rewriter.watermark.EnvironmentWithMutableCompositeWaterMark;
import io.bifroest.stream_rewriter.watermark.Watermark;

public class StreamRewriterEnvironment extends AbstractCommonEnvironment
implements EnvironmentWithMutablePersistentDrainManager,
           EnvironmentWithMutableRetentionStrategy,
           EnvironmentWithMutableDBInput,
           EnvironmentWithMutableNettyPortController,
           EnvironmentWithMutableCompositeWaterMark,
           EnvironmentWithMutableBifroestClient {

    private PersistentDrainManager<? extends Environment> persistentDrainManager;
    private RetentionConfiguration retentions;
    private BlockingQueue<Metric> dbQueue;
    private NettyPortController nettyPortController;
    private CompositeWatermark compositeWatermark;
    private BifroestClient bifroestClient;
            
    public StreamRewriterEnvironment( Path configPath, InitD init ) {
        super( configPath, init );
        compositeWatermark = new CompositeWatermark();
    }

    @Override
    public PersistentDrainManager<? extends Environment> persistentDrainManager() {
        return persistentDrainManager;
    }

    @Override
    public void setPersistentDrainManager(PersistentDrainManager<? extends Environment> persistentDrainManager) {
        this.persistentDrainManager = persistentDrainManager;
    }

    @Override
    public RetentionConfiguration retentions() {
        return retentions;
    }

    @Override
    public void setRetentions( RetentionConfiguration retentions ) {
        this.retentions = retentions;
    }

    @Override
    public NettyPortController nettyPortController() {
        return nettyPortController;
    }

    @Override
    public void setNettyPortController( NettyPortController nettyPortController ) {
        this.nettyPortController = nettyPortController;
    }

    @Override
    public Watermark watermark() {
        return compositeWatermark;
    }

    @Override
    public void addWatermark( Watermark wm ) {
        compositeWatermark.addWatermark( wm );
    }

    @Override
    public BlockingQueue<Metric> dbInputQueue() {
        return dbQueue;
    }

    @Override
    public void setDbInputQueue( BlockingQueue<Metric> queue ) {
        this.dbQueue = queue;
    }

    @Override
    public void setBifroestClient( BifroestClient bifroestClient ) {
        this.bifroestClient = bifroestClient;
    }

    @Override
    public BifroestClient bifroestClient() {
        return bifroestClient;
    }
}
