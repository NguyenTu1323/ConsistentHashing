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
    
    private static int scale = 100; // number of key to put per times
    
    
    public static void interactive(ClientWrapper clientWrapper){
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
            
            if(operation.equals("allkey")){
                for(int i = 0 ; i < 4*scale ; i++){
                    key = "key" + Integer.toString(i);
                    value = clientWrapper.get(key);
                    System.out.printf("(%s,%s)\n", key,value);
                }
            }
            
            if(operation.equals("exit")){
                break;
            }
            
            
        }
    }
    
    
    public static void interactiveSession() throws TException{
         
        Thread t1 = new Thread(){
            @Override
            public void run() {
                ClientWrapper clientWrapper1 = new ClientWrapper();
        
                clientWrapper1.init(2);
                
//                try {
//                    Thread.sleep(2000);
//                } catch (InterruptedException ex) {
//                    Logger.getLogger(ThriftClientJava.class.getName()).log(Level.SEVERE, null, ex);
//                }
//                
                for(int i = 0 ; i < scale ; i++){
                    clientWrapper1.put("key" + Integer.toString(i), "value" + Integer.toString(i));
                    //System.out.printf("t1 up\n");
                }
                
               clientWrapper1.init(1);
                
//                try {
//                    Thread.sleep(1000);
//                } catch (InterruptedException ex) {
//                    Logger.getLogger(ThriftClientJava.class.getName()).log(Level.SEVERE, null, ex);
//                }

                clientWrapper1.removeNode("Node1");
//                clientWrapper1.removeNode("Node3");
//                
                for(int i = scale ; i < 2*scale ; i++){
                    clientWrapper1.put("key" + Integer.toString(i), "value" + Integer.toString(i));
                    //System.out.printf("t1 down\n");
                }
                
                System.out.printf("%s",clientWrapper1.printSchema());
                
                
                interactive(clientWrapper1);
            }
            
        };
        
        Thread t2 = new Thread(){
            @Override
            public void run() {
                ClientWrapper clientWrapper2 = new ClientWrapper();
        
                clientWrapper2.init(2);
                
//                try {
//                    Thread.sleep(2000);
//                } catch (InterruptedException ex) {
//                    Logger.getLogger(ThriftClientJava.class.getName()).log(Level.SEVERE, null, ex);
//                }
                
                for(int i = 2*scale ; i < 3*scale ; i++){
                    clientWrapper2.put("key" + Integer.toString(i), "value" + Integer.toString(i));
                    //System.out.printf("t2 up\n");
                }       
                
               clientWrapper2.init(2);
               
               
//                try {
//                    Thread.sleep(2000);
//                } catch (InterruptedException ex) {
//                    Logger.getLogger(ThriftClientJava.class.getName()).log(Level.SEVERE, null, ex);
//                }
//                
                for(int i = 3*scale ; i < 4*scale ; i++){
                    clientWrapper2.put("key" + Integer.toString(i), "value" + Integer.toString(i));
                    //System.out.printf("t2 down\n");
                }
                
                System.out.printf("%s",clientWrapper2.printSchema());
                
                
                //interactive(clientWrapper2);
        
            }
            
        };
        
        t1.start();
        t2.start();
        
        // thằng này sẽ bị lỗi nếu như mà các NameNode hash ra cùng giá trị 
        
    }
    
    
    public static void main(String[] args){
        
        try {
            interactiveSession();
            
            
            
        } catch (TException ex) {
            Logger.getLogger(ThriftClientJava.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
}
