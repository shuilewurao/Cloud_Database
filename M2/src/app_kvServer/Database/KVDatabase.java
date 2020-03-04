package app_kvServer.Database;

import java.io.*;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Collections;

import java.util.ArrayList;
import org.apache.log4j.Logger;
import shared.Constants;
import shared.HashingFunction.MD5;
import shared.messages.KVMessage.StatusType;


public class KVDatabase implements IKVDatabase {

    private static final String DIR = Constants.DB_DIR;
    private static final String ESCAPER = Constants.ESCAPER;
    private static final String DELIM = Constants.DELIM;
    private static final String ESCAPED_ESCAPER = Constants.ESCAPED_ESCAPER;
    private static final String DELIMITER =  Constants.DELIMITER; // delimiter used by KVStore

    private int portNo;
    private String DBFileName;

    private String LUTName;
    private Map<String, KVEntry> synchLUT; //synchronized


    private Logger logger = Logger.getRootLogger();


    public KVDatabase(int portno) {
        this.DBFileName = "DB-Server" + portno + ".txt";
        this.synchLUT = null;
        this.LUTName = "LUT-" + portno + ".txt";
        this.portNo=portno;
        initializeDB();
    }

    private void initializeDB() {
        // open the storage file
        openFile();
        // open the Lookup Table file
        this.synchLUT = Collections.synchronizedMap(loadLUT());
    }


    public void clearStorage() {

        if (synchLUT != null) {
            synchLUT.clear();
            logger.info("Clear synchronized LUT");
        }

        // delete storage file
        try {
            File f = new File(getDBPath());
            boolean result = f.delete();
            if (!result) {
                logger.error("Unable to delete storage file");
            }else{
                logger.info("Storage file deleted successfully.");
            }
            logger.info("Storage file deleted successfully.");
            openFile();
        } catch (Exception x) {
            logger.error("File" + this.DBFileName + "can not be deleted");
        }

        // delete lookup table
        try {
            File f = new File(getLUTPath());
            boolean result = f.delete();
            if (!result) {
                logger.error("Unable to delete LUT file");
            }
            logger.info("Lookup Table deleted successfully.");
        } catch (Exception x) {
            logger.error("File" + this.LUTName + "can not be deleted");
        }

    }


    public String getKV(String Key) throws Exception {
        KVEntry kve = synchLUT.get(Key);
        if (kve != null) {//Find the KV pair in the Mapping Table
            byte[] results= readKVMsg(kve);
            //String val = byteArrayToValue(results);
            String[] content =  new String(results, StandardCharsets.UTF_8).split(DELIM);
            if(content.length==3 && content[0].getBytes(StandardCharsets.UTF_8)[0]==(byte)1){
                logger.info("Key: " + Key + "exist " + " in FileSystem");
                return decodeValue(content[2]).trim();
            }

            logger.debug("Key: " + Key + "not exist " + " in FileSystem");


            if(content.length>=1 && this.synchLUT.containsKey(Key) && content[0].getBytes(StandardCharsets.UTF_8)[0]==(byte)0){
                this.synchLUT.remove(Key);
            }
            return null;
        } else {
            logger.info("Key: " + Key + " does not exist " + " in FileSystem");
            return null;
        }
    }

    // TODO: handle this exception
    public StatusType putKV(String K, String V) throws Exception {
        KVEntry kve = synchLUT.get(K);
        StatusType status = StatusType.PUT_ERROR;
        try {
            if (V == null) {
                if (kve == null) {
                    logger.error("Try to delete an entry with non-exist key: " + K);
                    status = StatusType.DELETE_ERROR;
                    throw new IOException("Try to delete an entry with non-exist key: " + K);
                } else {
                    ModifyValidByte(kve.start_offset, kve.end_offset);
                    deleteKVEntry(K);
                    status = StatusType.DELETE_SUCCESS;
                    logger.info("Create [Key: " + K + ", Value: " + V + "] in FileSystem");
                }
            } else {
                byte[] msg = KVPairToBytes(K, V);
                appendEntry(msg, K);

                if (kve == null) {
                    status = StatusType.PUT_SUCCESS;
                    logger.info("Create [Key: " + K + ", Value: " + V + "] in FileSystem");
                } else {
                    status = StatusType.PUT_UPDATE;
                    logger.info("Update [Key: " + K + ", Value: " + V + "] in FileSystem");
                }
            }
            saveLUT();
        } finally {
            return status;
        }
    }

    private synchronized long appendEntry(byte[] bytes, String K) throws IOException{
        long location;

        RandomAccessFile raf = new RandomAccessFile(getDBPath(), "rw");
        location = raf.length();
        raf.seek(location);
        raf.write(bytes);
        raf.close();

        KVEntry added = new KVEntry(location, location + bytes.length);
        synchLUT.put(K, added);
        logger.info("Write Byte Array to disk");

        return location;
    }

    // TODO: Invalidate
    /*
    This invalidates an entry in storage
     */
    public synchronized boolean ModifyValidByte(long start, long end) throws IOException{

        RandomAccessFile raf = new RandomAccessFile(getDBPath(), "rw");
        raf.seek(start);
        //byte[] bytes = new byte[(int) (end - start)];
        byte[] bytes = new byte[(int) (1)];
        raf.read(bytes);

        bytes[0] = (byte)0;
        raf.seek(start);
        raf.write(bytes);
        raf.close();
        logger.info("Modify Valid Byte at Location: " + start);

        return true;
    }


    private synchronized void deleteKVEntry(String K) throws IOException {
        synchLUT.remove(K);
        logger.info("Delete Key: " + K + " from FileSystem");
    }

    public boolean inStorage(String K) {
        KVEntry kve = synchLUT.get(K);
        return (kve != null);
    }

    public String getDBPath() {
        return this.DIR + "/" + this.DBFileName;
    }

    public String getLUTPath() {
        return this.DIR + "/" + this.LUTName;
    }

    private void openFile() {

        logger.info("Initialize iterate storage file ...");
        boolean fileDNE;
        try {
            // create directory of persisted storage
            File dir = new File(this.DIR);
            if (!dir.exists()) {
                boolean mkdir_result = dir.mkdir();
                if (!mkdir_result) {
                    logger.error("Unable to create file " + this.DIR);
                    return;
                }
            }

            // create the storage file for current server service
            File tempDBfile = new File(getDBPath());

            fileDNE = tempDBfile.createNewFile();
            if (fileDNE) {
                logger.info("New storage file created");
            } else {
                logger.info("Storage file found");
            }

            File tempLUTfile = new File(getLUTPath());

            fileDNE = tempLUTfile.createNewFile();
            if (fileDNE) {
                logger.info("New LUT file created");
            } else {
                logger.info("LUT file found");
            }

        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Error when trying to initialize file instance", e);
        }
    }

    private Map loadLUT() {
        Map<String, KVEntry> tmpLUT = new HashMap<String, KVEntry>();
        try {
            File lut_file = new File(getLUTPath()); //here you make a filehandler - not a filesystem file.

            if (!lut_file.exists() || lut_file.length()==0) {
                return tmpLUT;
            }
            FileInputStream fileIn = new FileInputStream(getLUTPath());
            ObjectInputStream in = new ObjectInputStream(fileIn);
            tmpLUT = (Map<String, KVEntry>) in.readObject();
            in.close();
            fileIn.close();
            logger.info("Lookup Table is loaded.");
        } catch (IOException i) {
            i.printStackTrace();
            logger.error(" IOException: Load LookUp Table");
        } catch (ClassNotFoundException c) {
            logger.error("Load LookUp Table ClassNotFoundException");
            c.printStackTrace();
        } finally {
            // TODO: Flow
            return tmpLUT;
        }
    }

    private boolean saveLUT() {
        try {
            FileOutputStream fileOut =
                    new FileOutputStream(getLUTPath(), false);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(this.synchLUT);
            out.close();
            fileOut.close();
            logger.info("Serialized data is saved in " + LUTName);

        } catch (IOException i) {
            i.printStackTrace();
            logger.error("Load LookUp Table IOException");

        }
        return false;
    }


    private synchronized byte[] readKVMsg(KVEntry kve) throws IOException{
        byte[] bytes = null;
        try {
            RandomAccessFile raf = new RandomAccessFile(getDBPath(), "r");
            raf.seek(kve.start_offset);
            bytes = new byte[(int) (kve.end_offset - kve.start_offset)];
            raf.read(bytes);
            raf.close();
            logger.info("Find Byte Array From disk");
            return bytes;
        } catch (IOException e) {
            logger.error("Read disk failed");
            throw e;
        }
    }

    private String byteArrayToValue(byte[] Bytes){

        String[] content =  new String(Bytes, StandardCharsets.UTF_8).split(DELIM);
        if(content.length==3 && content[0].getBytes(StandardCharsets.UTF_8)[0]==(byte)1){
            return decodeValue(content[2]).trim();
        }
        return null;
    }

    private String byteArrayToKV(byte[] Bytes){

        String[] content =  new String(Bytes, StandardCharsets.UTF_8).split(DELIM);
        if(content.length==3 && content[0].getBytes(StandardCharsets.UTF_8)[0]==(byte)1){
            return decodeValue(content[1]) + DELIM + decodeValue(content[2]).trim() + "\r\n";
        }
        return null;
    }

    // TODO: exception
    private byte[] KVPairToBytes(String key, String value) throws IOException {
        byte valid = (byte)1;
        String validity = new String(new byte[]{valid}, StandardCharsets.UTF_8);
        return (validity+ DELIM + encodeValue(key) + DELIM + encodeValue(value) + "\r\n").getBytes("UTF-8");
    }

    private String encodeValue(String value) {
        return value.replaceAll("\r", "\\\\r")
                .replaceAll("\n", "\\\\n")
                .replaceAll(ESCAPER, ESCAPED_ESCAPER);
    }

    private String decodeValue(String value) {
        return value.replaceAll("\\\\r", "\r")
                .replaceAll("\\\\n", "\n")
                .replaceAll(ESCAPED_ESCAPER, ESCAPER);
    }


    /**
     * Get the data to be moved
     * @param hashRange
     * @return
     * @throws Exception
     */
    public String getPreMovedData(String[] hashRange) throws Exception {

        String startRange=hashRange[0];
        String endRange=hashRange[1];
        //assume I get port Number and Address
        ArrayList<Byte> ByteArray;
        // using for-each loop for iteration over Map.entrySet()
        StringBuilder stringList= new StringBuilder();
        logger.debug("Get Hash Range from "+hashRange[0]+" to "+hashRange[1]);

        for (Map.Entry<String,KVEntry> entry : synchLUT.entrySet())
        {
            BigInteger key= MD5.HashInBI(entry.getKey());
            KVEntry kve= entry.getValue();
            //System.out.println("Key: "+entry.getKey()+ " in Server:"+this.PortNumber%10);
            logger.debug("Key " +entry.getKey()+ " in port:"+this.portNo);

            if(MD5.isKeyinRange(key,startRange,endRange))//Check for key in range or not
            {

                if(kve.isValid()==false){
                    logger.debug("Move an invalid KV entry");
                    // TODO: may need to restore the LUT log
                }else{
                    // valid bit checking
                    logger.debug("[DB] Move an valid KV entry");
                    byte[] result= readKVMsg(kve);
                    logger.debug("[DB] "+result.toString());
                    String[] tokens =  new String(result, StandardCharsets.UTF_8).split(DELIM);
                    String copied_str;
                    if(tokens[0].getBytes(StandardCharsets.UTF_8)[0]==(byte)1) {
                        if (tokens.length == 3) {
                            copied_str = decodeValue(tokens[1]) + DELIMITER +decodeValue(tokens[2]) + "\r\n";
                        } else if (tokens.length == 2) {
                            copied_str = decodeValue(tokens[1]) + "\r\n";
                        } else {
                            logger.debug("An invalid kve in Database/");
                            continue;
                        }
                        stringList.append(copied_str);
                    }
                }
            }
        }

        String result = stringList.toString();
        logger.debug("[DB] sent" + result);

        return result;
    }


    public synchronized boolean deleteKVPairByRange(String[]hashRange)
    {

        try {
            String startRange = hashRange[0];
            String endRange = hashRange[1];
            logger.info("Remove Keys from look up table from " + startRange + " to" + endRange);

            ArrayList<KVEntry> toDelete = new ArrayList<>();
            for (Map.Entry<String, KVEntry> entry : synchLUT.entrySet()) {

                BigInteger key = MD5.HashInBI(entry.getKey());
                KVEntry kve = entry.getValue();
                if (MD5.isKeyinRange(key, startRange, endRange))//Check for key in range or not
                {
                    ModifyValidByte(kve.start_offset, kve.end_offset);
                    synchLUT.remove(entry.getKey());
                    logger.debug("Delete Key: " + key);
                }
            }
            saveLUT();

            return true;
        }catch(IOException ioe){
            logger.debug("Unable to delete KV Pair By range");
            return false;
        }

    }

    public boolean receiveTransferdData(String content){
        System.out.println("Transfer data:" + content);
        String[] kv_pairs = content.split("\\\r\n");

        try{
            for (String kv: kv_pairs){
                String[] k_v = kv.split("\\"+DELIMITER);
                // As PUT
                byte[] bytes = KVPairToBytes(k_v[0].trim(), k_v[1].trim());
                //System.out.println("Key-Value: [" +kv+"]");
                appendEntry(bytes, k_v[0].trim());
            }
            saveLUT();
            logger.info("Data has been moved to server"+this.portNo);
            return true;

        }catch(IOException e){
            logger.error("Unable to make transfer data to server:"+this.portNo);
            return false;
        }



    }


}