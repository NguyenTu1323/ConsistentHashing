/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package thriftclientjava;

import consistentHashing.thriftStuff.ConsistentHashingThriftService;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 *
 * @author cpu10663-local
 */
public class ClientWrapper {
    
    
    private ConsistentHashingThriftService.Client client ;
    private TTransport transport;
    private TProtocol protocol;
    
    
    public ClientWrapper(){
        try {
            
            transport = new TSocket("localhost",9090);
            
            transport.open();
            
            protocol = new TBinaryProtocol(transport);
            
            client = new ConsistentHashingThriftService.Client(protocol);
            
            
            
        } catch (TTransportException ex) {
            Logger.getLogger(ClientWrapper.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
    public boolean init(int numberOfNodes){
        try {
            boolean flag = client.init(numberOfNodes);
            return flag;
        } catch (TException ex) {
            Logger.getLogger(ClientWrapper.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
    }
    
    public boolean put(String key, String value){
        try {
            boolean flag = client.put(key, value);
            return flag;
        } catch (TException ex) {
            Logger.getLogger(ClientWrapper.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
    }
    
    public String get(String key){
        try {
            String val = client.get(key);
            return val;
        } catch (TException ex) {
            Logger.getLogger(ClientWrapper.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
    }
    
    public boolean addNode(String nodeName){
        try {
            boolean flag =  client.addNode(nodeName);
            return flag;
        } catch (TException ex) {
            Logger.getLogger(ClientWrapper.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
    }
    
    public boolean removeNode(String nodeName){
        try {
            boolean flag = client.removeNode(nodeName);
            return flag;
        } catch (TException ex) {
            Logger.getLogger(ClientWrapper.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
    }
    
     public String printSchema() {
        try {
            String val = client.printSchema();
            return val;
        } catch (TException ex) {
            Logger.getLogger(ClientWrapper.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
     }

    boolean removeKey(String key) {
        try {
            boolean flag =  client.removeKey(key);
            return flag;
        } catch (TException ex) {
            Logger.getLogger(ClientWrapper.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
    }
    
}
