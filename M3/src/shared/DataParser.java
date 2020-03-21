package shared;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.log4j.Logger;
import shared.HashingFunction.MD5;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class parses data in enron data set to the format
 * of KV String
 */
public class DataParser {
    private static Logger logger = Logger.getRootLogger();
    public static final String DATA_ROOT = "/nfs/ug/homes-2/c/chenxu23/ECE419/maildir";

    /**
     * Convert all data files under certain dir under DATA_ROOT
     * to KVMessage format
     *
     * @param dir dir under DATA_ROOT to search for
     * @return List of KVMessages
     */
    public static List<String> parseDataFrom(String dir) {
        Collection<File> files = FileUtils.listFiles(new File(DATA_ROOT + "/" + dir),
                new RegexFileFilter("^(\\d*?)\\."),
                DirectoryFileFilter.DIRECTORY);

        ArrayList<String> result = new ArrayList<>();

        for (File file : files) {
            try {
                StringBuilder val = new StringBuilder();
                FileReader fileReader = new FileReader(file);
                BufferedReader reader = new BufferedReader(fileReader);
                String line;

                while ((line = reader.readLine()) != null) {
                    val.append(line);
                }

                reader.close();
                fileReader.close();

                String key = Objects.requireNonNull(MD5.HashInBI(val.toString())).toString();
                String value = val.toString().replaceAll("[^a-zA-Z0-9]", "");

                if (checkKeyValue(key, value))
                    result.add(key + Constants.DELIMITER + value);

            } catch (FileNotFoundException e) {
                logger.error(file.getAbsolutePath() + " not found!");
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        assert result.size() > 0;
        return result;
    }

    /**
     * Convert all data files under dirs under DATA_ROOT which start with
     * the starting char to KVMessage format
     *
     * @param startingChar the starting char of dir
     * @return List of String
     */
    public static List<String> parseDataFrom(Character startingChar) {
        File file = new File(DATA_ROOT);
        String[] dirs = file.list((dir, name) ->
                (name.startsWith(String.valueOf(startingChar)))
                        && (new File(dir, name).isDirectory()));

        ArrayList<String> result = new ArrayList<>();
        assert dirs != null;
        for (String dir : dirs) {
            assert parseDataFrom(dir) != null;
            result.addAll(Objects.requireNonNull(parseDataFrom(dir)));
        }
        return result;
    }

    private static boolean checkKeyValue(String key, String value) {

        if (key.length() > Constants.BUFFER_SIZE) {
            return false;
        }

        if (value.length() > Constants.DROP_SIZE) {
            return false;
        }

        Pattern p = Pattern.compile("[^A-Za-z0-9]");
        Matcher m = p.matcher(key);

        if (m.find()) {
            return false;
        }

        // not delete
        if (!value.isEmpty()) {
            m = p.matcher(value);
            return !m.find();
        }
        return true;
    }
}
