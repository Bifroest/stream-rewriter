package io.bifroest.stream_rewriter.netty;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.time.Clock;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.MDC;

import io.bifroest.commons.logging.LogService;
import io.bifroest.commons.model.Metric;
import io.bifroest.commons.statistics.WriteToStorageEvent;
import io.bifroest.commons.statistics.eventbus.EventBusManager;
import io.bifroest.commons.statistics.storage.MetricStorage;
import io.bifroest.stream_rewriter.db.EnvironmentWithDBInput;

@Sharable
public class NettyHandler<E extends EnvironmentWithDBInput> extends SimpleChannelInboundHandler<String> {
    private static final Logger log = LogService.getLogger(NettyHandler.class);

    private static final AtomicLong metricsOutputted = new AtomicLong();

    private final E environment;

    public NettyHandler( E environment ) {
        this.environment = environment;

        EventBusManager.createRegistrationPoint().subscribe( WriteToStorageEvent.class, w -> {
            MetricStorage storage = w.storageToWriteTo().getSubStorageCalled( "netty" );
            storage.store( "metrics-to-dbnator", metricsOutputted.longValue() );
        });
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, String line) {
        MDC.put( "thread", "NettyHandler" );
        log.trace( line );

        String lineparts[] = StringUtils.split( line );

        if( lineparts.length != 2 && lineparts.length != 3 ) {
            throw new UnsupportedOperationException( "Cannot parse line " + line );
        }

        String metricName = lineparts[0];
        try {
            double value = Double.valueOf( lineparts[1] );
            long timestamp = lineparts.length == 3
                    ? Long.parseLong( lineparts[2] )
                    : Clock.systemUTC().instant().getEpochSecond();

            Metric metric = new Metric( metricName, timestamp, value );

            environment.dbInputQueue().put( metric );

            long metrics = metricsOutputted.incrementAndGet();
            if ( metrics % 1000 == 0 ) {
                log.debug( metrics + " metrics outputted!" );
            }
        } catch ( NumberFormatException e ) {
            log.warn( "Cannot parse numbers in line " + line, e );
        } catch( InterruptedException e ) {
            log.warn( "Interrupted while adding metric to queue " + line, e );
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
        super.channelReadComplete( ctx );
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        ctx.close();
        super.channelInactive( ctx );
    };

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.warn( "Exception in NettyHandler", cause );
        ctx.close();
        super.exceptionCaught( ctx, cause );
    }
}
