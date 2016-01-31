package io.bifroest.stream_rewriter.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.util.Arrays;
import java.util.Collection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.kohsuke.MetaInfServices;

import io.bifroest.commons.SystemIdentifiers;
import io.bifroest.commons.boot.interfaces.Subsystem;
import io.bifroest.commons.configuration.EnvironmentWithJSONConfiguration;
import io.bifroest.stream_rewriter.StreamRewriterIdentifiers;
import io.bifroest.stream_rewriter.db.EnvironmentWithDBInput;

@MetaInfServices
public class NettySystem<E extends EnvironmentWithJSONConfiguration & EnvironmentWithDBInput & EnvironmentWithMutableNettyPortController> implements Subsystem<E>, NettyPortController {
    public static final Logger log = LogManager.getLogger();

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ServerBootstrap b;
    private ChannelFuture future;
    private int threadCount;
    private int port;

    @Override
    public String getSystemIdentifier() {
        return StreamRewriterIdentifiers.NETTY;
    }

    @Override
    public Collection<String> getRequiredSystems() {
        return Arrays.asList( SystemIdentifiers.STATISTICS, StreamRewriterIdentifiers.DBNATOR );
    }

    @Override
    public synchronized void boot( E environment ) throws Exception {
        environment.setNettyPortController( this );

        InternalLoggerFactory.setDefaultFactory( new Log4J2LoggerFactory( ) );

        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup( threadCount );

        b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
         .channel(NioServerSocketChannel.class)
         .childHandler(new NettyInitializer<E>( environment ) );

        openNettyPort();
    }

    @Override
    public synchronized void shutdown( E environment ) {
        if ( isPortOpen() ) {
            try {
                closeNettyPort();
            } catch( InterruptedException e ) {
                log.warn( "Interrupted while closing netty boss group!", e );
            }
        }
        try {
            bossGroup.shutdownGracefully().sync();
        } catch( InterruptedException e ) {
            log.warn( "Interrupted while closing netty boss group!", e );
        }
        try {
            workerGroup.shutdownGracefully().sync();
        } catch( InterruptedException e ) {
            log.warn( "Interrupted while closing netty worker group!", e );
        }
    }

    @Override
    public void configure(JSONObject configuration) {
        JSONObject subconfig = configuration.getJSONObject( "netty" );
        threadCount = subconfig.getInt( "thread-count" );
        port = subconfig.getInt( "port" );
    }

    @Override
    public synchronized void openNettyPort() throws InterruptedException {
        if ( isPortOpen() ) {
            throw new IllegalStateException( "The netty port is already open!" );
        }

        log.info( "Opening port!" );

        future = b.bind( port ).sync();
    }

    @Override
    public synchronized void closeNettyPort() throws InterruptedException {
        if ( !isPortOpen() ) {
            throw new IllegalStateException( "The netty port is already closed!" );
        }

        log.info( "Closing port!" );

        future.channel().close();
        future.channel().closeFuture().sync();

        log.debug( "Port closed!" );

        future = null;
    }

    @Override
    public boolean isPortOpen() {
        return ( future != null );
    }
}
