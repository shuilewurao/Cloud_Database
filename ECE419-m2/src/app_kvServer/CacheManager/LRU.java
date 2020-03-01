package app_kvServer.CacheManager;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;


public class LRU extends CachePolicy {

    public LRU(int cacheSize) {
        super(cacheSize);
        this.cacheList = Collections.synchronizedMap(
                        new LinkedHashMap(
                                cacheSize,
                                0.75f,
                                true
                        ) {
                            @Override
                            protected boolean removeEldestEntry(Map.Entry eldest) {
                                return size() > getCacheSize();
                            }
                        }
        );
    }

}