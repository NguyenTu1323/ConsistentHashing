/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package consistentHashing.thriftStuff;

import consistentHashing.ConsistentHashingModule.ConsistentHashing;
import org.apache.thrift.TException;

/**
 *
 * @author cpu10663-local
 */
public class ConsistentHashingThriftServiceHandler implements ConsistentHashingThriftService.Iface{
    
    
    public ConsistentHashing hashingModule ;
    
    public ConsistentHashingThriftServiceHandler(ConsistentHashing hashingModule){
        this.hashingModule = hashingModule;
    }
    
    
     @Override
    public boolean removeKey(String key) throws TException {
        return this.hashingModule.removeKey(key);
    }
    
    @Override
    public boolean init(int numberOfNodes) throws TException {
        return hashingModule.init(numberOfNodes);
    }

    @Override
    public boolean addNode() throws TException {
        return hashingModule.addMasterNode() != null;
    }

    @Override
    public boolean removeNode(String nodeName) throws TException {
        return hashingModule.removeNode(nodeName);
    }

    @Override
    public String printSchema() throws TException {
        return hashingModule.printSchema();
    }

    @Override
    public boolean put(String key, String value) throws TException {
        return hashingModule.put(key,value);
    }

    @Override
    public String get(String key) throws TException {
        return hashingModule.get(key);
    }
    
    
    
}
