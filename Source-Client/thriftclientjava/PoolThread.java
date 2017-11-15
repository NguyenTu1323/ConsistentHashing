/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package thriftclientjava;

import thriftclientjava.Tasks.Tasks;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author cpu10663-local
 */
public class PoolThread extends Thread {
    
    private BlockingQueue<Tasks> blockingQueue;
    private boolean isStopped = false;
    private ClientWrapper clientWrapper = null;
    
    public PoolThread(BlockingQueue<Tasks> blockingQueue) {
        this.blockingQueue = blockingQueue;
        // every thread got a connection
        this.clientWrapper = new ClientWrapper();
        this.clientWrapper.initConnection();
    }

    @Override
    public void run() {
        while(isStopped == false){
            try {
                Tasks task = blockingQueue.take();
                System.out.printf("%s\n",Thread.currentThread().getName());
                if(task != null){
                    // pass clientWrapper to task , would not open connection again and again
                    task.ReceiveClientWrapper(clientWrapper);
                    task.run();
                }
                else{
                    throw new NullPointerException("Task is null.");
                }
                
                
            } catch (InterruptedException ex) {
                Logger.getLogger(PoolThread.class.getName()).log(Level.SEVERE, null, ex);
            }
            
        }
    }
    
    public void doStop(){
        isStopped = true;
        clientWrapper.closeConnection();
        this.interrupt();
    }
    
    
    
}
