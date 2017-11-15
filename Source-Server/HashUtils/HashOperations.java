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
        
        int offset = 8;
        
        int stride = 1;
        
        for(int i = offset ; i < offset+stride*9 ; i+=1){
            long tmp = byteArray[i];
            long mask = (tmp << (6*(i-offset)));
            mask = mask <0 ? -mask : mask;
            res = res | mask;
            //long cc = res | mask;
            //System.out.printf("tmp = %d  mask = %d  res = %d  cc = %d\n",tmp,mask,res,cc);
        }
        
        return res < 0 ? -res : res ; // lay res duong
    }
    
    
}
