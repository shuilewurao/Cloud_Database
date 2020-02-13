package app_kvServer.Database;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
import java.util.Collections;
import java.io.ObjectOutputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.FileInputStream;
import java.io.File;

import java.util.ArrayList;
import org.apache.log4j.Logger;
import shared.HashingFunction.MD5;
import shared.messages.KVMessage.StatusType;


public class KVDatabase implements IKVDatabase {

    private static final String dir = "./MyStore";
    private static final String ESCAPER = "-";
    private static final String DELIM = ESCAPER + ",";
    private static final String ESCAPED_ESCAPER = ESCAPER + "d";

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
            byte[] result = readKVMsg(kve);
            String val = byteArrayToValue(result);
            logger.info("Key: " + Key + "exist " + " in FileSystem");
            return val;
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
                    ModifyValidByte(kve.start_offset, kve.end_offset);
                    status = StatusType.PUT_UPDATE;
                    logger.info("Update [Key: " + K + ", Value: " + V + "] in FileSystem");
                }
            }
            saveLUT();
            // TODO
//        } catch (Exception x) {
//            logger.error("Exception when handling PUT in FileSystem");
//            status = StatusType.PUT_ERROR;
        } finally {
            return status;
        }
    }

    private synchronized long appendEntry(byte[] bytes, String K) throws IOException{
        long location;
        //try {
            RandomAccessFile raf = new RandomAccessFile(getDBPath(), "rw");
            location = raf.length();
            raf.seek(location);
            raf.write(bytes);
            raf.close();

            KVEntry added = new KVEntry(location, location + bytes.length);
            synchLUT.put(K, added);
            logger.info("Write Byte Array to disk");

        //} catch (IOException e) {
//            logger.error("Write to disk failed");
//            return -1;
//        }
        return location;
    }

    // TODO: Invalidate
    /*
    This invalidates an entry in storage
     */
    public synchronized boolean ModifyValidByte(long start, long end) throws IOException{
        //try {
            RandomAccessFile raf = new RandomAccessFile(getDBPath(), "rw");
            raf.seek(start);
            byte[] bytes = new byte[(int) (end - start)];
            raf.read(bytes);
            //for (int i=0;i<bytes.length;i++)
            //{
            //   System.out.println(bytes[i]);
            // }
            bytes[4] = 0; // TODO
            raf.seek(start);
            raf.write(bytes);
            raf.close();
            logger.info("Modify Valid Byte at Location: " + start);

//        } catch (IOException e) {
//            logger.error("Modify Valid Byte Failed");
//            return false;
//        }
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
        return this.dir + "/" + this.DBFileName;
    }

    public String getLUTPath() {
        return this.dir + "/" + this.LUTName;
    }

    private void openFile() {

        logger.info("Initialize iterate storage file ...");
        boolean fileDNE;
        try {
            // create directory of persisted storage
            File dir = new File(this.dir);
            if (!dir.exists()) {
                boolean mkdir_result = dir.mkdir();
                if (!mkdir_result) {
                    logger.error("Unable to create file " + this.dir);
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
            //System.out.println("Serialized data is saved in lookuptable.txt");
        } catch (IOException i) {
            i.printStackTrace();
            logger.error("Load LookUp Table IOException");

        }
        return false;
    }

    // TODO: exception
    private byte[] KVPairToBytes(String key, String value) throws IOException {

        String message = "\r\r\r\r"; //Initial Identifier
        byte[] MessageBytes = message.getBytes();
        //byte []Validity = new byte[(byte)(Status?1:0)]; //TODO
        byte[] Validity = new byte[(byte) (1)];
        //Contain Identifer and Validity
        byte[] tmp = new byte[MessageBytes.length + 1];
        System.arraycopy(MessageBytes, 0, tmp, 0, MessageBytes.length);
        System.arraycopy(Validity, 0, tmp, MessageBytes.length, 1); // TODO


        //KeyLength Bytes Array

        byte[] KeyByteArray = key.getBytes();
        int KeyLength = KeyByteArray.length;
        byte[] KeyLengthBytes = ByteBuffer.allocate(4).putInt(KeyLength).array();
        // int pomAsInt = ByteBuffer.wrap(bytes).getInt();

        byte[] tmp1 = new byte[KeyLengthBytes.length + KeyByteArray.length];
        System.arraycopy(KeyLengthBytes, 0, tmp1, 0, KeyLengthBytes.length);
        System.arraycopy(KeyByteArray, 0, tmp1, KeyLengthBytes.length, KeyByteArray.length);

        //ValueByteArray
        byte[] ValueByteArray = value.getBytes();
        int ValueLength = ValueByteArray.length;
        byte[] ValueLengthBytes = ByteBuffer.allocate(4).putInt(ValueLength).array();
        // Cast back methods int pomAsInt = ByteBuffer.wrap(bytes).getInt();
        byte[] tmp2 = new byte[ValueByteArray.length + ValueLengthBytes.length];
        System.arraycopy(ValueLengthBytes, 0, tmp2, 0, ValueLengthBytes.length);
        System.arraycopy(ValueByteArray, 0, tmp2, ValueLengthBytes.length, ValueByteArray.length);

        byte[] finalArray = new byte[tmp.length + tmp1.length + tmp2.length];
        System.arraycopy(tmp, 0, finalArray, 0, tmp.length);
        System.arraycopy(tmp1, 0, finalArray, tmp.length, tmp1.length);
        System.arraycopy(tmp2, 0, finalArray, tmp1.length + tmp.length, tmp2.length);


        logger.info("Convert Key: " + key + " to Byte Array in FileSystem");

        return finalArray;
        // return (encodeValue(key) + DELIM + encodeValue(val) + "\r\n").getBytes("UTF-8");
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
        //Check Correct format of the Bytes Array
        int counter = 0;
        int i;
        for (i = 0; i < 4; i++) if (Bytes[counter++] != 0xD) return "";//3
        if (Bytes[++counter] != 0) return "";//4 TODO invalid entry
        byte[] KeyLength = new byte[4];
        for (i = 0; i < 4; i++)
            KeyLength[i] = Bytes[counter++];//8
        int keylength = ByteBuffer.wrap(KeyLength).getInt();
        counter += keylength;
        byte[] ValueLength = new byte[4];
        for (i = 0; i < 4; i++)
            ValueLength[i] = Bytes[counter++];//8
        int valuelength = ByteBuffer.wrap(ValueLength).getInt();

        byte[] Value = new byte[valuelength];
        for (i = 0; i < valuelength; i++) {
            Value[i] = Bytes[counter + i];
        }
        logger.info("Convert Byte Array to " + new String(Value).trim() + " in FileSystem");

        return new String(Value).trim();

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
        StringBuilder Stringlist= new StringBuilder();
        logger.debug("Get Hash Range from "+hashRange[0]+" to "+hashRange[1]);
        //System.out.println("Get Hash Range from "+hashRange[0]+" to "+hashRange[1]);

        for (Map.Entry<String,KVEntry> entry : synchLUT.entrySet())
        {

            BigInteger key= MD5.HashInBI(entry.getKey());
            KVEntry kve= entry.getValue();
            //System.out.println("Key: "+entry.getKey()+ " in Server:"+this.PortNumber%10);
            logger.debug("Key "+entry.getKey()+ "in port:"+this.portNo);

            if(MD5.IsKeyinRange(key,startRange,endRange))//Check for key in range or not
            {

                byte[] result= readKVMsg(kve);
                String kvresult = new String(result, "UTF-8");
                Stringlist.append(kvresult);
                logger.debug("Send Key: "+entry.getKey());
            }
        }
        String result = Stringlist.toString();

        return result;
    }


    public synchronized boolean deleteKVPairByRange(String[]hashRange) throws Exception
    {

        String startRange=hashRange[0];
        String endRange=hashRange[1];
        logger.info("Remove Keys from look up table from "+startRange+" to"+endRange);

        ArrayList<String> toDelete = new ArrayList<>();
        for (Map.Entry<String,KVEntry> entry : synchLUT.entrySet())
        {

            BigInteger key=MD5.HashInBI(entry.getKey());
            KVEntry kve= entry.getValue();
            if(MD5.IsKeyinRange(key,startRange,endRange))//Check for key in range or not
            {
                toDelete.add(entry.getKey());
                //synchronizedMap.remove(entry.getKey());
            }
        }
        for (String key : toDelete){
            synchLUT.remove(key);
            logger.debug("Delete Key: "+ key);
        }
        saveLUT();

        return true;

    }


}