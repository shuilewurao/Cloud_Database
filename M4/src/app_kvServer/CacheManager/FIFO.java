package app_kvServer.CacheManager;

import java.util.*;


public class FIFO extends CachePolicy {

    public FIFO(int cacheSize) {
        super(cacheSize);
        this.cacheList = Collections.synchronizedMap(
                new LinkedHashMap<String, String>(
                        cacheSize
                ) {
                    @Override
                    protected boolean removeEldestEntry(Map.Entry eldest) {
                        return size() > getCacheSize();
                    }
                }
        );
    }

}