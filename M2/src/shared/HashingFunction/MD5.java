package shared.HashingFunction;

import java.security.*;
import java.math.*;

public class MD5
{
    public static BigInteger HashInBI(String s) throws NoSuchAlgorithmException {
        MessageDigest m = MessageDigest.getInstance("MD5");
        m.update(s.getBytes(),0,s.length());
        return new BigInteger(1,m.digest());
    }

    public static String HashInStr(String s) throws NoSuchAlgorithmException {
        MessageDigest m = MessageDigest.getInstance("MD5");
        m.update(s.getBytes(),0,s.length());
        String hashStr = new BigInteger(1,m.digest()).toString(16);

        if(hashStr.length() < 32)
        {
            int padding = 32 - hashStr.length();
            // null padding
            String pad = new String(new char[padding]).replace("\0", "0");
            hashStr = pad + hashStr;
        }
        return hashStr;
    }


    public static boolean IsKeyinRange(BigInteger keyHash, String StartHash, String Endhash)

    {
        //String Minvalue = new String(new char[32]).replace("\0", "0");
        //String MaxValue = new String(new char[32]).replace("\0", "f");
        BigInteger upper = new BigInteger(StartHash);
        BigInteger lower = new BigInteger(Endhash);
        //BigInteger keyHash= new BigInteger(keyhash);

        boolean descend = upper.compareTo(lower) == 1;
        if(keyHash.compareTo(upper) == 0 || keyHash.compareTo(lower) == 0){
            return true;
        }else if(upper.compareTo(lower) == -1 && keyHash.compareTo(upper) == 1 && keyHash.compareTo(lower) == -1){
            return true;
        }else if(upper.compareTo(lower) == 1 && keyHash.compareTo(upper) == -1 && keyHash.compareTo(lower) == 1
        ){ return true;
        }

        return false;

    }

}