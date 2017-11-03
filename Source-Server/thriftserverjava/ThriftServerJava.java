/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package thriftserverjava;

import consistentHashing.ConsistentHashingModule.ConsistentHashing;
import consistentHashing.thriftStuff.ConsistentHashingThriftService;
import consistentHashing.thriftStuff.ConsistentHashingThriftServiceHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import jdk.nashorn.internal.runtime.regexp.joni.constants.NodeType;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;

/**
 *
 * @author cpu10663-local
 */
public class ThriftServerJava {

    //public static MultiplicationHandler handler;
    public static ConsistentHashingThriftService.Processor processor;
    
    public static ConsistentHashingThriftServiceHandler handler;
    
    public static ConsistentHashing hashingModule;
    
    
    public static void main(String[] args) {
        // TODO code application logic here
        
        //handler = new MultiplicationHandler();
        
        
        hashingModule = new ConsistentHashing();
        
        handler = new ConsistentHashingThriftServiceHandler(hashingModule);
        
        processor = new ConsistentHashingThriftService.Processor(handler);
        
        Runnable t1 = new Runnable() {
            @Override
            public void run() {
                simple(processor);
            }
        };
        
        new Thread(t1).start();
        
    }
    
    public static void simple(ConsistentHashingThriftService.Processor processor){
        try {
            TServerTransport serverTransport = new TServerSocket(9090);
            TServer server = new TSimpleServer(new Args(serverTransport).processor(processor));
            
            System.out.printf("Server started serving at 9090\n");
            server.serve();
        } catch (TTransportException ex) {
            Logger.getLogger(ThriftServerJava.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
}
