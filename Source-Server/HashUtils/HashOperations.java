/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package HashUtils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 *
 * @author cpu10663-local
 */
public class HashOperations {
    
    public static MessageDigest hashFunction = null;
    
    
    public static MessageDigest getInstance() {
        try{
            if(hashFunction == null){
                hashFunction = MessageDigest.getInstance("SHA1");
            }
            return hashFunction;
        }catch(Exception e){
            return null;
        }
    }
    
    public static long hash(String key){
        
        // nhan vao string, hash roi sau do tra ve 1 so long 64 bit
        MessageDigest function = getInstance();
        synchronized(function){
            // @@ tat ca moi thang goi hash  deu dung chung function object , khong synchronized no la no override buffer cua nhau =>> hash sai bet 
            // vi cai nay ma ton 1 ngay debug , fuck
            
            function.update(key.getBytes());
        
            byte[] byteArray = function.digest();


            long result = truncatedAndGet64Bit(byteArray);

            return result;
        
        }
        
    }

    private static long truncatedAndGet64Bit(byte[] byteArray) {
        // lay 8 byte dau
        // that ra lay 7 thoi
        long res = 0;
        
        int offset = 12;
        
        for(int i = offset ; i < offset+7 ; i++){
            long tmp = byteArray[i];
            res = res | (tmp << (8*(i-offset)));
        }
        
        return res < 0 ? -res : res ; // lay res duong
    }
    
    
}
