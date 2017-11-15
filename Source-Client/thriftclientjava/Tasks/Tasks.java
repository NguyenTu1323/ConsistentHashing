/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package thriftclientjava.Tasks;

import thriftclientjava.ClientWrapper;

/**
 *
 * @author cpu10663-local
 */
public abstract class Tasks implements Runnable{
    
    public ClientWrapper clientWrapper;
    
    public Tasks(){
        //clientWrapper = new ClientWrapper();
    }
    
    public void ReceiveClientWrapper(ClientWrapper clientWrapper){
        this.clientWrapper = clientWrapper;
    }
    
    
    
}
