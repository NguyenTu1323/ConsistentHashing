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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author cpu10663-local
 */
public class ConsistentHashing {

    public static Integer genID = 0;
    public ConcurrentMap<Long,ConsistentNode> nodes;
    public List<Long> nodeHashValues;
    public ConcurrentMap<Long,Long> masters;
    
    
    public ConsistentHashing(){
        this.nodes = new ConcurrentHashMap<>();
        this.nodeHashValues = new ArrayList<Long>();
        this.masters = new ConcurrentHashMap<>();
    }
    
    public boolean init(int numberOfNodes) {
        
        // hyper-parameter , tuning here
        int numberOfVirtualNodes = 3;
        
        try{
            for(int i = 0 ; i < numberOfNodes ; i++){
                String masterNodeName =  addMasterNode();
                
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
            
            //fibo(45);
            
            long hashValue = HashOperations.hash(key);
            
            // lock o day vi co truong hop sau khi xac dinh owner node ,
            // neu co node khac duoc add vao dan den owner node thay doi 
            // ma se khong duoc cap nhat =>> put vao nham owner node
            // =>> luc look-up se khong thay
            synchronized(nodeHashValues){
                ConsistentNode ownerNode = getNodeBasedOnHashValue(hashValue);
            //log 
                System.out.printf("put (%s,%s) successfully\n",key,value);
            
                return ownerNode.put(key,value);
            }
            
            
        }catch(Exception e){
            
            System.out.printf("put (%s,%s) fail\n", key,value);
            
            return false;
        }
    }

    private ConsistentNode getNodeBasedOnHashValue(long hashValue) {
        try {
            // binary search here
            
            //int masterHashValue = getMasterHashValue(hashValue);
            
            // tim node nao ma thang hashValue nay thuoc ve, co the la master , co the la virual node
            int nodeIndex;
            long hashValueOfNodeIndex;
            synchronized(nodeHashValues){
                nodeIndex = getNodeIdBasedOnHashValue(hashValue);
            
            // lay hashValue cua master cua node do
                hashValueOfNodeIndex = nodeHashValues.get(nodeIndex);
            }
            
            long hashValueMaster = getHashValueOfMaster(hashValueOfNodeIndex);
            
            
            return nodes.get(hashValueMaster);
            
            
        } catch (NoSuchFieldException ex) {
            
            Logger.getLogger(ConsistentHashing.class.getName()).log(Level.SEVERE, null, ex);
            return  null;
        }
        
        
        
    }

    private int AddNodeHashValueIntoList(long hashValue) {
        int i = 0 ;
        synchronized(nodeHashValues){
            for(i = 0 ; i < nodeHashValues.size() ;){
                if(nodeHashValues.get(i) < hashValue){
                    i++;
                }
                else{
                    break;
                }
            }
            nodeHashValues.add(i, hashValue);
        }
        
        return i;
    }

    private int getNodeIdBasedOnHashValue(long hashValue) {
        // trong luc chay binary search thi khong cho ai thay doi gi hashValue
        // bin search nen chay cung nhanh , ko so no la bottleneck O(logn)
        synchronized(nodeHashValues){
            int l = 0 ;
            int r = nodeHashValues.size() - 1;
            int mid , flag = 1;

            if(nodeHashValues.get(r) < hashValue)
                return 0;

            while(l < r){
                mid = l + (r-l)/2 ;
                if((flag = 1 - flag) == 1)
                    mid++;
                if(hashValue <= nodeHashValues.get(mid))
                    r = mid;
                else
                    l = mid +1;
            }
            return l;
        }
        
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

    public String addMasterNode() {
        try{
            String nodeName = generateNodeName();
            System.out.printf("in addMaster : %s\n", nodeName);
            long hashValue = HashOperations.hash(nodeName);
            //synchronized(masters){
                 masters.put(hashValue, hashValue);   
            //}
    
            
            //synchronized(nodes){
                nodes.put(hashValue,new ConsistentNode(nodeName, hashValue));
            //}
            

            int idNodeNextInList;
            // move cac key cua node nay di 
            int idNodeInList ;
            
            long hashMaster;
            synchronized(nodeHashValues){
                // in case another thread adding new nodes , id of current node may be modified in nodeHashValues =>> lock
                idNodeInList = AddNodeHashValueIntoList(hashValue);
                idNodeNextInList = getIdNextNodeInList(idNodeInList);
                hashMaster = getHashValueOfMaster(nodeHashValues.get(idNodeNextInList));   
                
                ConsistentNode node = nodes.get(hashMaster);
            
                RearrangeKey(node);
        
            }
            
            // log 
            System.out.printf("add node (nodename = %s) successfully\n",nodeName);

            return nodeName;
            
        }catch(Exception e){
            
            // log 
            System.out.printf("add node fail\n");
            
            
            return null;
        }
    }

    private String generateNodeName() {
        synchronized(genID){
            return "Node" + Integer.toString(genID++);
        }
    }

    private int getIdNextNodeInList(int idNodeInList) {
            // in case other threads modify nodeHashValues
        synchronized(nodeHashValues){
            return (idNodeInList + 1) % nodeHashValues.size();
        }
    }

    private void RearrangeKey(ConsistentNode node) {
        // phan phoi cac key cua node hien tai vao cac node        
        try{
            System.out.printf("Rearrange key of %s\n", node.nodeName);
            
            
            Map<String,String> databases = node.databases;
            node.databases = null;
            node.databases = new ConcurrentHashMap<String,String>();

            for(String key : databases.keySet()){
                System.out.printf("-------key = %s\n",key);
                put(key, databases.get(key));
            }
            
        }catch(Exception e){
            // if node is null , execution flow jumps directly to here
            // and do nothing :)
        }

        
        
        
    }

    public boolean removeNode(String masterNodeName) {
        try{
            
            long hashValueMasterNode = HashOperations.hash(masterNodeName);
            ConsistentNode removedNode;
            
            //synchronized(nodes){
                removedNode = nodes.get(hashValueMasterNode);
            //}
     
            if(removedNode == null){
                // node nay khong co trong danh sach node hoac da bi remove
                return true;
            }
            
            List<Long> hashValueVirtualNodeLists = removedNode.getListHashValueOfVirtualNodes();
            ConcurrentMap<String,String> databaseRemovedNode = removedNode.databases;
            
            synchronized(nodeHashValues){
                // vi remove cac element trong nodeHashValues nen lock no lai 
                nodeHashValues.remove(hashValueMasterNode);
                for(int i = 0 ; i < hashValueVirtualNodeLists.size() ; i++){
                    nodeHashValues.remove(hashValueVirtualNodeLists.get(i));
                }
            }
            
                       
            // ok da remove xong node , h dem cac (key,value) phan phat lai vao cac node khac 
            
            //synchronized(databaseRemovedNode){
                for(String key : databaseRemovedNode.keySet()){
                    put(key,databaseRemovedNode.get(key));
                }
            //}
            
            
            
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
        
        
        // khong can lock nodes o day 
  
        synchronized(nodeHashValues){
            for(int i = 0 ; i < nodeHashValues.size() ; i++){
                if(nodes.containsKey(nodeHashValues.get(i))){
                    String schemaNode = nodes.get(nodeHashValues.get(i)).printSchema();
                    stringBuffer.append(schemaNode);
                }
            }
        }
        stringBuffer.append("---------------------------------------------------------------------\n\n");
        
        // log
        System.out.printf("printSchema successfully\n");
        
        return stringBuffer.toString();
    }

    private long getHashValueOfMaster(Long hashValue) throws NoSuchFieldException {
        if(masters.containsKey(hashValue)){
            return masters.get(hashValue);
        }
        else{
            throw new NoSuchFieldException("masters does not contains hashValue\n");
        }
    }

    public boolean removeKey(String key) {
        long hashValue = HashOperations.hash(key);
        ConsistentNode node = getNodeBasedOnHashValue(hashValue);
        node.remove(key);
        return true;
    }

    private void addVirtualNode(String masterNodeName) {
        // add vao masters de mot tra cho nhanh
        try{
            long hashMaster = HashOperations.hash(masterNodeName);
            ConsistentNode masterNode = nodes.get(hashMaster);
            String virtualNodeName = masterNode.generatedVirtualNodeName();
            long hashVirtual = HashOperations.hash(virtualNodeName);
            
            masters.put(hashVirtual,hashMaster);

            
            int idNodeInList;
            int idNodeNextInList;
            long hashMasterNodeNext ;
            // khong cho 2 thang cung add virtualNode 1 luc 
            // neu khong thi may cai id cua tui no se lon xon
            // =>> put key vao nham node =>> look up khong thay mac du da save key lai , nhung no nam o node khac
            synchronized(nodeHashValues){
                idNodeInList = AddNodeHashValueIntoList(hashVirtual);
                idNodeNextInList = getIdNextNodeInList(idNodeInList);
                hashMasterNodeNext = getHashValueOfMaster(nodeHashValues.get(idNodeNextInList));
                
            }
            
            
            ConsistentNode node = nodes.get(hashMasterNodeNext);

            RearrangeKey(node);
            
        }catch(Exception e){
            // do nothing 
        }
        
    }
    
    
    
    
}
