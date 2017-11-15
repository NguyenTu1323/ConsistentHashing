/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package thriftclientjava.Tasks;

import thriftclientjava.Tasks.Tasks;

/**
 *
 * @author cpu10663-local
 */
public class RemoveNodeTask extends Tasks{
    
    private String nodeName;
    
    public RemoveNodeTask(String nodeName){
        this.nodeName = nodeName;
    }
    
    @Override
    public void run() {
        clientWrapper.removeNode(nodeName);
    }
    
}
