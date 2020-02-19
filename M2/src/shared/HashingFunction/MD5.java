package shared.HashingFunction;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.log4j.Logger;
import shared.Constants;

public class MD5
{
    private static Logger logger = Logger.getRootLogger();

    public static BigInteger HashInBI(String s){
        try{
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(s.getBytes(),0,s.length());
            return new BigInteger(1,md.digest());

        }catch (NoSuchAlgorithmException e){
            //logger.fatal("Unable to find hashing algorithm", e);
            e.printStackTrace();
            return null;
        }
    }

    public static String HashInStr(String s) {
        try{
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(s.getBytes(),0,s.length());

            String hashStr = new BigInteger(1,md.digest()).toString(16);

            if(hashStr.length() < 32)
            {
                int padding = 32 - hashStr.length();
                // zero padding
                String pad = new String(new char[padding]).replace("\0", "0");
                hashStr = pad + hashStr;
            }
            return hashStr;


        }catch (NoSuchAlgorithmException e){
            logger.fatal("Unable to find hashing algorithm", e);
            e.printStackTrace();
            return null;
        }
    }


    public static BigInteger HashFromHostAddress(String host, int port){

        assert host != null;
        assert port != -1;

        String val = host + Constants.HASH_DELIMITER + port;

        return MD5.HashInBI(val);

    }


    public static boolean isKeyinRange(BigInteger keyHash, String StartHash, String Endhash)

    {

        BigInteger upper = new BigInteger(StartHash);
        BigInteger lower = new BigInteger(Endhash);

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