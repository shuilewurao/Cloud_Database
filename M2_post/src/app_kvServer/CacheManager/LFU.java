package app_kvServer.CacheManager;

import java.util.Collections;
import java.util.*;

public class LFU extends CachePolicy {

    private Map<String, Integer> frequencyList;

    public LFU(int cacheSize) {
        super(cacheSize);
        this.cacheList = Collections.synchronizedMap(
                        new LinkedHashMap<String, String>(cacheSize, 0.75f, true));
        this.frequencyList = Collections.synchronizedMap(
                        new TreeMap<String, Integer>());
    }

    /*
    src: https://javahungry.blogspot.com/2017/11/how-to-sort-treemap-by-value-in-java.html
     */
    public static <K, V extends Comparable<V>> Map<K, V>
    sortByValues(final Map<K, V> map) {
        Comparator<K> valueComparator =
                new Comparator<K>() {
                    public int compare(K k1, K k2) {
                        int compare =
                                map.get(k1).compareTo(map.get(k2));
                        if (compare == 0)
                            return 1;
                        else
                            return compare;
                    }
                };

        Map<K, V> sortedByValues =
                new TreeMap<K, V>(valueComparator);
        sortedByValues.putAll(map);
        return sortedByValues;
    }

    private void frequencyUpdate(String key) {
        synchronized (frequencyList) {
            Integer newFrequency = frequencyList.get(key) + 1;
            frequencyList.put(key, newFrequency);
        }
    }

    @Override
    public String getKV(String key) {
        synchronized (cacheList) {
            synchronized (frequencyList) {
                if (cacheList.containsKey(key)) {
                    frequencyUpdate(key);
                    return cacheList.get(key);
                }
                else {
                    return null;
                }
            }
        }
    }

    @Override
    public void putKV(String key, String value) {
        synchronized (cacheList) {
            synchronized (frequencyList) {
                if(value == null){
                    if(inCache(key)){
                        deleteCache(key);
                    }
                }else{
                    // update
                    if(frequencyList.containsKey(key)){
                        cacheList.put(key, value);
                        frequencyUpdate(key);
                    }
                    // add
                    else{
                        if (maxCacheSize == cacheList.size()) {
                            evict();// Call Make space function
                        }
                        cacheList.put(key, value);
                        frequencyList.put(key, 1);
                    }

                }
            }
        }
    }


    private void evict() {
        Map sortedMap = sortByValues(frequencyList);
        Map.Entry temp = (Map.Entry) sortedMap.entrySet().iterator().next();
        frequencyList.remove(temp.getKey());
        cacheList.remove(temp.getKey());
    }


    @Override
    public void clearCache() {
        cacheList.clear();
        frequencyList.clear();
    }

    @Override
    protected void deleteCache(String key) {
        synchronized (cacheList) {
            synchronized (frequencyList) {
                cacheList.remove(key);
                frequencyList.remove(key);
            }
        }
    }
}