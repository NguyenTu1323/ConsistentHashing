/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package thriftclientjava;

import thriftclientjava.Tasks.Tasks;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 *
 * @author cpu10663-local
 */
class ThreadPool {
    
    private List<PoolThread> threads = new ArrayList<>();
    private BlockingQueue<Tasks> blockingQueue;

    public ThreadPool(int maxThreads, BlockingQueue<Tasks> blockingQueue) {
        this.blockingQueue = blockingQueue;
        for(int i = 0 ; i < maxThreads ; i++){
            threads.add(new PoolThread(blockingQueue));
        }
        
        for(int i = 0 ; i< threads.size() ; i++){
            threads.get(i).start();
        }
    }
    
    public void execute(Tasks task){
        blockingQueue.add(task);
    }
    
    
    
}
