/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.goodgame.profiling.stream_rewriter.forking;

import com.goodgame.profiling.drains.Drain;
import com.goodgame.profiling.stream_rewriter.source.handler.StreamLineHandler;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author hkraemer@ggs-hh.net
 */
public class ClientHandler implements Runnable {
    private static final Logger log = LogManager.getLogger();
    
    private final Socket client;
    private final StreamLineHandler lineHandler;
    private final Drain drain;
    
    ClientHandler( Socket client, Drain d ) {
        log.debug( "Got connection from " + client.getRemoteSocketAddress() );
        this.client = client;
        this.drain = d;
        this.lineHandler = new StreamLineHandler( d );
    }

    @Override
    public void run() {
        try {
            BufferedReader fromClient = new BufferedReader( new InputStreamReader( client.getInputStream() ));
            while( true ) {
                String line = fromClient.readLine();
                if ( line == null ) break;
                try {
                    this.lineHandler.handleUnit( line );
                } catch ( Exception e  ) {
                    log.warn( "Error handling line " + line, e);
                }
            }
        } catch ( IOException e  ) {
            log.warn( "Error while serving client ", e);
        } finally {
            log.debug( "Connection from " + client.getRemoteSocketAddress() + " closed" );
            try {
                this.drain.close();
            } catch ( IOException e  ) {
                log.error( "Error flushing metrics", e );
            }
            try {
                client.close();
            } catch ( IOException e ) {
                log.error( "Cannot close client port, giving up ", e );
            }
        }
    }
    
}
