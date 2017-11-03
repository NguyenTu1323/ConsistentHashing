/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package consistentHashing.ConsistentHashingModule;

import HashUtils.HashOperations;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 *
 * @author cpu10663-local
 */
public class ConsistentHashing {

    public static int genID = 0;
    
    public Map<Long,ConsistentNode> nodes;
    
    public List<Long> nodeHashValues;
    
    public Map<Long,Long> masters;
    
    
    public ConsistentHashing(){
        this.nodes = new HashMap<Long,ConsistentNode>();
        this.nodeHashValues = new ArrayList<Long>();
        this.masters = new HashMap<>();
    }
    
    public boolean init(int numberOfNodes) {
        
        // hyper-parameter , tuning here
        int numberOfVirtualNodes = 5;
        
        try{
            for(int i = 0 ; i < numberOfNodes ; i++){
                // add master node
                String masterNodeName = generateNodeName();
                addMasterNode(masterNodeName);
                
                // add virtual nodes
                for(int j = 0 ; j < numberOfVirtualNodes ; j++){
                    addVirtualNode(masterNodeName);
                }
                
            }    
            
            // log 
            System.out.printf("init %d nodes complete\n",numberOfNodes);
            
            return true;
            
        }catch(Exception e){
            //log
            System.out.printf("init fault\n");
            
            
            return false;
        }
            
    }

    public boolean put(String key, String value) {
        try{
            long hashValue = HashOperations.hash(key);
            ConsistentNode ownerNode = getNodeBasedOnHashValue(hashValue);
            
            
            //log 
            System.out.printf("put (%s,%s) successfully\n",key,value);
            
            return ownerNode.put(key,value);
            
        }catch(Exception e){
            
            System.out.printf("put (%s,%s) fail\n", key,value);
            
            return false;
        }
    }

    private ConsistentNode getNodeBasedOnHashValue(long hashValue) {
        // binary search here 
        
        //int masterHashValue = getMasterHashValue(hashValue);
        
        // tim node nao ma thang hashValue nay thuoc ve, co the la master , co the la virual node
        int nodeIndex = getNodeIdBasedOnHashValue(hashValue);
        
        // lay hashValue cua master cua node do
        long hashValueMaster = getHashValueOfMaster(nodeHashValues.get(nodeIndex));
        
        
        return nodes.get(hashValueMaster);
        
        
        
    }

    private int AddNodeHashValueIntoList(long hashValue) {
        // test ham nay
        int i = 0 ;
        for(i = 0 ; i < nodeHashValues.size() ;){
            if(nodeHashValues.get(i) < hashValue){
                i++;
            }
            else{
                break;
            }
        }
        nodeHashValues.add(i, hashValue);
        return i;
    }

    private int getNodeIdBasedOnHashValue(long hashValue) {
        int l = 0 ; 
        int r = nodeHashValues.size() - 1;
        int mid , flag = 1;
        
        if(nodeHashValues.get(r) < hashValue)
            return 0;
        
        while(l < r){
            mid = l + (r-l)/2 ;
            flag = 1 - flag;
            if(hashValue <= nodeHashValues.get(mid))
                r = mid;
            else
                l = mid +1;
        }
        return l;
        
    }

    public String get(String key) {
        try{
            long hashValue = HashOperations.hash(key);
            ConsistentNode ownerNode = getNodeBasedOnHashValue(hashValue);
            
            
            
            return ownerNode.get(key);
        }
        catch(Exception e){
            
            // log
            System.out.printf("get key (%s) fail\n",key);
            
            return null;
        }
    }

    public boolean addMasterNode(String nodeName) {
        try{   
            long hashValue = HashOperations.hash(nodeName);
            masters.put(hashValue, hashValue);
            int idNodeInList = AddNodeHashValueIntoList(hashValue);
            nodes.put(hashValue,new ConsistentNode(nodeName, hashValue));

            int idNodeNextInList = getIdNextNodeInList(idNodeInList);
            // move cac key cua node nay di 
            
            long hashMaster = getHashValueOfMaster(nodeHashValues.get(idNodeNextInList));

            //ConsistentNode node = nodes.get(nodeHashValues.get(idNodeNextInList));
            
            ConsistentNode node = nodes.get(hashMaster);

            RearrangeKey(node);
            
            
            // log 
            System.out.printf("add node (nodename = %s) successfully\n",nodeName);

            return true;
            
        }catch(Exception e){
            
            // log 
            System.out.printf("add node fail\n");
            
            
            return false;
        }
    }

    private String generateNodeName() {
        return "Node" + Integer.toString(genID++);
    }

    private int getIdNextNodeInList(int idNodeInList) {
        return (idNodeInList + 1) % nodeHashValues.size();
    }

    private void RearrangeKey(ConsistentNode node) {
        // phan phoi cac key cua node hien tai vao cac node
        
        
//        List<String> removeKey = new ArrayList<String>();
//        
//        for(String key : node.databases.keySet()){
//            // duyet tat cac cac (key,value) hien co cua node
//            long hashValueOfKey = HashOperations.hash(key); // lay hashvalue cua key
//            int nodeIdCurrentKey = getNodeIdBasedOnHashValue(hashValueOfKey); // xem key nay se thuoc ve node moi nao
//            if(nodeIdCurrentKey != nodeId){
//                // add cai nay vao nodeIdCurrentKey 
//                ConsistentNode nodeCurrentKey = nodes.get(nodeHashValues.get(nodeIdCurrentKey)); // lay cai node ma key nay thuoc ve 
//                nodeCurrentKey.put(key,node.get(key)); // add key vao node moi
//  
//                // add vao de ty remove key ra khoi node cu
//                removeKey.add(key);
//            }
//        }
//        
//        for(String key : removeKey){
//            node.remove(key);
//        }
          
        Map<String,String> databases = node.databases;
        node.databases = null;
        node.databases = new HashMap<>();
        
        for(String key : databases.keySet()){
            put(key, databases.get(key));
        }
        
        
    }

    public boolean removeNode(String masterNodeName) {
        try{
            
//            long hashValueOfNode = HashOperations.hash(nodeName); // lay hashValue cua node dem di remove
//            int removedNodeId = getNodeIdBasedOnHashValue(hashValueOfNode);
//        
//            ConsistentNode removeNode = nodes.get(nodeHashValues.get(removedNodeId)); // tim cai node de remove
//
//            Map<String,String> databaseRemovedNode = removeNode.databases;
//            
//            nodeHashValues.remove(removedNodeId);
//            
//            nodes.remove(hashValueOfNode);

            long hashValueMasterNode = HashOperations.hash(masterNodeName);
            ConsistentNode removedNode = nodes.get(hashValueMasterNode);
            List<Long> hashValueVirtualNodeLists = removedNode.getListHashValueOfVirtualNodes();
            Map<String,String> databaseRemovedNode = removedNode.databases;
            
            nodeHashValues.remove(hashValueMasterNode);
            for(int i = 0 ; i < hashValueVirtualNodeLists.size() ; i++){
                nodeHashValues.remove(hashValueVirtualNodeLists.get(i));
            }
            
            nodes.remove(hashValueMasterNode);
//            
            // ok da remove xong node , h dem cac (key,value) phan phat lai vao cac node khac 
            
            
            for(String key : databaseRemovedNode.keySet()){
//                long hashValueCurrentKey = HashOperations.hash(key);
//                ConsistentNode node = getNodeBasedOnHashValue(hashValueCurrentKey);
                //node.put(key, databaseRemovedNode.get(key));
                
                put(key,databaseRemovedNode.get(key));
            }
            
            
            // log 
            System.out.printf("remove node (nodename = %s) succcessfully\n", masterNodeName);
            
            return true;
            
        }catch(Exception e){
            
            // log 
            System.out.printf("remove node (nodename = %s) fail\n", masterNodeName);
            
            return false;
        }
        
    }

    public String printSchema() {
        StringBuffer stringBuffer = new StringBuffer();
        for(int i = 0 ; i < nodeHashValues.size() ; i++){
            if(nodes.containsKey(nodeHashValues.get(i))){
                String schemaNode = nodes.get(nodeHashValues.get(i)).printSchema();
                stringBuffer.append(schemaNode);
            }
        }
        stringBuffer.append("---------------------------------------------------------------------\n\n");
        
        // log
        System.out.printf("printSchema successfully\n");
        
        return stringBuffer.toString();
    }

    private long getHashValueOfMaster(Long hashValue) {
        return masters.get(hashValue);
    }

    public boolean removeKey(String key) {
        long hashValue = HashOperations.hash(key);
        ConsistentNode node = getNodeBasedOnHashValue(hashValue);
        node.remove(key);
        return true;
    }

    private void addVirtualNode(String masterNodeName) {
        // add vao masters de mot tra cho nhanh
        long hashMaster = HashOperations.hash(masterNodeName);
        ConsistentNode masterNode = nodes.get(hashMaster);
        String virtualNodeName = masterNode.generatedVirtualNodeName();
        long hashVirtual = HashOperations.hash(virtualNodeName);
        masters.put(hashVirtual,hashMaster);
        
        
        
        int idNodeInList = AddNodeHashValueIntoList(hashVirtual);
        int idNodeNextInList = getIdNextNodeInList(idNodeInList);
        
        long hashMasterNodeNext = getHashValueOfMaster(nodeHashValues.get(idNodeNextInList));
        ConsistentNode node = nodes.get(hashMasterNodeNext);
        
        RearrangeKey(node);
    }
    
    
    
    
}
