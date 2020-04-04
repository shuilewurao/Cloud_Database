package app_kvServer.CacheManager;

import java.util.Map;

public abstract class CachePolicy {
    protected int maxCacheSize;

    protected Map<String, String> cacheList;

    public CachePolicy(int cachesize) {
        this.maxCacheSize = cachesize;
    }

    public boolean inCache(String key) {
        synchronized (cacheList) {
            return cacheList.containsKey(key);
        }
    }

    protected void deleteCache(String key) {
        synchronized (cacheList) {
            cacheList.remove(key);
        }
    }

    public String getKV(String key) {
        synchronized (cacheList) {
            if (cacheList.containsKey(key)) {
                return cacheList.get(key);
            } else {
                return null;
            }
        }
    }

    public void putKV(String key, String value) {
        synchronized (cacheList) {
            //TODO: invalid value

            if (value == null || value.equals("")) {
                if (inCache(key)) {
                    deleteCache(key);
                }
            } else {
                cacheList.put(key, value);
            }
        }
    }

    public void clearCache() {
        cacheList.clear();
    }

    protected int getCacheSize() {
        return this.maxCacheSize;
    }

}