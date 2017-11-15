/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package consistentHashing.ConsistentHashingModule;

import HashUtils.HashOperations;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 * @author cpu10663-local
 */
public class ConsistentNode {
    
    public String nodeName ;
    
    public long hashValue;
    
    public ConcurrentMap<String,String> databases ;
    
    private int id_gen;

    ConsistentNode(String nodeName, long hashValue) {
        this.nodeName = nodeName;
        this.hashValue = hashValue;
        this.databases = new ConcurrentHashMap<>();
        this.id_gen = 0;
    }
    
    
    public List<Long> getListHashValueOfVirtualNodes(){
        List<Long> listHash = new ArrayList<>();
        // khong can dat lock o day , neu id_gen ma co tang trong luc vong lap dang chay thi cung khong anh huong gi toi hoa binh the gioi :v
        for(int i = 0 ; i < this.id_gen ; i++){
            listHash.add(HashOperations.hash(generatedVirtualNodeName(i)));
        }
        return listHash;
    }
    
    public boolean put(String key, String value) {
        try{
            synchronized(databases){
                databases.put(key, value);
                return true;
            }
        }
        catch(Exception e){
            return false;
        }
    }

    public String get(String key) {
        try{
            synchronized(databases){
                if(databases.containsKey(key)){
                    return databases.get(key);
                }
                else{
                    // log
                    System.out.printf("databases do not contains your query key\n");
                    return null;
                }
            }
        }
        catch(Exception e){
            return null;
        }
    }

    void remove(String key) {
        synchronized(databases){
            if(databases.containsKey(key)){
                databases.remove(key);
            }
        }
        
    }

    String printSchema() {
        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append(String.format("NodeName : %s , hashValue : %d\n",nodeName,hashValue));
        synchronized(databases){
            for(String key : databases.keySet()){
                stringBuffer.append(String.format("----- (%s , %s) hash = %d \n", key,databases.get(key),HashOperations.hash(key)));
            }
        }
        stringBuffer.append("\n");
        return stringBuffer.toString();
    }

    public String generatedVirtualNodeName() {
        synchronized(this){
            // lock id_gen , khong cho 2 thang increase cung luc
            return  this.nodeName + ".Virtual" + Integer.toString(id_gen++);   
        }
    }

    private String generatedVirtualNodeName(int i) {
        return  this.nodeName + ".Virtual" + Integer.toString(i);
    }
    
    
    
}
