package com.dotcms.hazelcast.mapstore;

import java.util.Properties;

import com.hazelcast.core.MapStore;
import com.hazelcast.core.MapStoreFactory;

public class DotH22MapStoreFactory implements MapStoreFactory {
    @Override
    public MapStore<String, Object> newMapStore(String mapName, Properties properties) {
        return new H22RegionMapStore(mapName);
    }
}
