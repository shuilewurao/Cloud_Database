package shared.HashingFunction;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.log4j.Logger;
import shared.Constants;

public class MD5 {
    private static Logger logger = Logger.getRootLogger();

    public static BigInteger HashInBI(String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(s.getBytes(), 0, s.length());
            return new BigInteger(1, md.digest());

        } catch (NoSuchAlgorithmException e) {
            //logger.fatal("Unable to find hashing algorithm", e);
            e.printStackTrace();
            return null;
        }
    }


    public static BigInteger HashFromHostAddress(String host, int port) {

        assert host != null;
        assert port != -1;

        String val = host + Constants.HASH_DELIMITER + port;

        return MD5.HashInBI(val);

    }


    public static boolean isKeyinRange(BigInteger keyHash, String StartHash, String Endhash) {

        BigInteger start = MD5.HashInBI(StartHash);
        BigInteger end = MD5.HashInBI(Endhash);

        assert start != null;
        assert end != null;
        if (keyHash.compareTo(start) == 0 || keyHash.compareTo(end) == 0) {
            return true;
        } else if (start.compareTo(end) < 0) {
            return keyHash.compareTo(start) > 0 && keyHash.compareTo(end) < 0;
        } else { //  upper.compareTo(lower) > 0
            return keyHash.compareTo(start) > 0 || keyHash.compareTo(end) < 0;
        }

    }

}