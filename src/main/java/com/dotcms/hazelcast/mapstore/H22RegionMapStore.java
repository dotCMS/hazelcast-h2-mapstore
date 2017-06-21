package com.dotcms.hazelcast.mapstore;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;

import com.hazelcast.core.MapStore;



public class H22RegionMapStore implements MapStore<String, Object> {

    static H22MapStoreStorage cache;
    final String region;
    final String path;
    public H22RegionMapStore(String region) {
        this(region, null);
    }

    public H22RegionMapStore(String region, String tmpPath) {
        super();
        this.region = region;
       

        if (tmpPath == null) {
            File tempDir = new File(HazelH2PropertyBundle.getProperty("mapstore.h2.database.folder", "H22MapStore"));
            tempDir.mkdirs();
            tmpPath = tempDir.getAbsolutePath();
        }
        this.path = tmpPath;
        initCache();

        
    }

    private  synchronized void initCache(){
        if (cache == null) {
            Logger logger = Logger.getLogger(this.getClass().getName());
            logger.info("Building H22MapStore:" + path);
            System.out.println("Building H22MapStore : " + path);


            this.cache = new H22MapStoreStorage(path);
            try{
                this.cache.init();
            }
            catch(Exception e){
                throw new RuntimeException(e);
            }

        }
    }
    
    

    @Override
    public Object load(String key) {
        return cache.get(region, key);
    }

    @Override
    public Map<String, Object> loadAll(Collection<String> keys) {
        Map<String, Object> map = new HashMap<>();
        for (String key : loadAllKeys()) {
            map.put(key, load(key));
        }
        return map;
    }

    @Override
    public Iterable<String> loadAllKeys() {
        return cache.getKeys(region);
    }

    @Override
    public void store(String key, Object value) {
        cache.put(region, key, value);

    }

    @Override
    public void storeAll(Map<String, Object> map) {
        for (Entry<String, Object> entry : map.entrySet()) {
            store(entry.getKey(), entry.getValue());
        }

    }

    @Override
    public void delete(String key) {
        cache.remove(region, key);

    }

    @Override
    public void deleteAll(Collection<String> keys) {
        for (String key : keys) {
            delete(key);
        }

    }

}
