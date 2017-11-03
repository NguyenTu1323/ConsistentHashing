/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package thriftclientjava;

import consistentHashing.thriftStuff.ConsistentHashingThriftService;
import java.util.Scanner;
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
public class ThriftClientJava {

    /**
     * @param args the command line arguments
     */
    
    public static ClientWrapper clientWrapper;
    
    
    public static void interactive(){
        Scanner in = new Scanner(System.in);
        
        
        String operation;
        String key;
        String value ;
        String masterNodeName;
        String schema;
        boolean flag = false;
        
        while(true){
            operation = in.next();
            
            if(operation.equals("addnode")){
                masterNodeName = in.next();
                flag = clientWrapper.addNode(masterNodeName);
                if(flag == true){
                    System.out.printf("add node %s successfully\n", masterNodeName);
                }
                else{
                    System.out.printf("add node %s fail\n",masterNodeName);
                }
            }
            
            
            if(operation.equals("removenode")){
                masterNodeName = in.next();
                flag = clientWrapper.removeNode(masterNodeName);
                if(flag == true){
                    System.out.printf("remove node %s successfully\n", masterNodeName);
                }
                else{
                    System.out.printf("remove node %s fail\n",masterNodeName);
                }
            }
            
            
            if(operation.equals("get")){
                key = in.next();
                value = clientWrapper.get(key);
                if(value != null){
                    System.out.printf("(%s) has value (%s)\n",key,value);
                }
                else{
                    System.out.printf("no key(%s) found\n",key);
                }
            }
            
            if(operation.equals("put")){
                key = in.next();
                value = in.nextLine();
                flag = clientWrapper.put(key, value);
                if(flag == true){
                    System.out.printf("put (%s,%s) successfully\n",key,value);
                }
                else{
                    System.out.printf("put (%s,%s) fail\n", key,value);
                }
            }
            
            if(operation.equals("removekey")){
                key = in.next();
                flag = clientWrapper.removeKey(key);
                if(flag == true){
                    System.out.printf("remove key(%s) successfully\n",key);
                }
                else{
                    System.out.printf("remove key(%s) fail\n", key);
                }
            }
            
            
            if(operation.equals("schema")){
                schema = clientWrapper.printSchema();
                System.out.printf("%s", schema);
            }
            
            if(operation.equals("exit")){
                break;
            }
            
            
        }
    }
    
    
    public static void interactiveSession() throws TException{
        
        
        clientWrapper = new ClientWrapper();
        
        clientWrapper.init(5);
        
        for(int i = 0 ; i < 30 ; i++){
            clientWrapper.put("key" + Integer.toString(i), "value" + Integer.toString(i));
        }
       
        
        interactive();
        
    }
    
    
    public static void main(String[] args){
        
        try {
            interactiveSession();
            
            
            
        } catch (TException ex) {
            Logger.getLogger(ThriftClientJava.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
}
