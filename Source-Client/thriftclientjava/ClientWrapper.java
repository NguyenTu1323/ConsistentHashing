/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package thriftclientjava;

import consistentHashing.thriftStuff.ConsistentHashingThriftService;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 *
 * @author cpu10663-local
 */
public class ClientWrapper {
    
    
    public ConsistentHashingThriftService.Client client = null;
    private TTransport transport = null;
    private TProtocol protocol = null;
    
    
    public ClientWrapper(){
        
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
            if(client == null){
                // try to init connection again 
                initConnection();
                // if success client is not null now 
            }
            boolean flag = client.put(key, value);
            //System.out.printf("%s\n", key);
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
    
    public boolean addNode(){
        try {
            boolean flag =  client.addNode();
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

    
    public void closeConnection(){
        transport.close();
    }
    
    public void initConnection() {
        try {
            
            transport = new TSocket("localhost",9090);
            
            // if machine surpress the number of connection it can create(1024 in this case) , transport.open() will deny and throw an exception 
            transport.open();
            
            
            protocol = new TBinaryProtocol(transport);
            
            client = new ConsistentHashingThriftService.Client(protocol);
          
          
          
        } catch (TTransportException ex) {
            // case cannot open connection 
            Logger.getLogger(ClientWrapper.class.getName()).log(Level.SEVERE, null, ex);
            String cause = ex.getMessage();
            System.out.printf("%s\n", cause);
            
        }
    }
    
}
