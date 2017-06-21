package com.dotcms.hazelcast.mapstore;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.junit.Test;


import com.hazelcast.core.MapStore;



public class MapStoreTest {
    
    final String[] GROUPNAMES = { "testGroup", "testGroup2", "myBigGroup" };
    final String KEYNAME = "testKey";
    final String CONTENT = "test my Content!!!";
    final String LONG_GROUPNAME = "thifsais-as-disadisaidsadiias-dsaidisadisaid";
    final String LONG_KEYNAME = new String(
            "A"
                    + "\u00ea"
                    + "\u00f1"
                    + "\u00fc"
                    + "C"
                    + "ASDSADFDDSFasfddsadasdsadsadasdq3r4efwqrrqwerqewrqewrqreqwreqwrqewrqwerqewrqewrqwerqwerqwerqewr43545324542354235243524354325423qwerewds fds fds gf eqgq ewg qeg qe wg egw    ww  eeR ASdsadsadadsadsadsadaqrewq43223t14@#$#@^%$%&%#$sfwf erqwfewfqewfgqewdsfqewtr243fq43f4q444fa4ferfrearge");;
    final String CANT_CACHE_KEYNAME = "CantCacheMe";
    final int numberOfPuts = 5000;
    final int numberOfThreads = 40;
    final int numberOfGroups = 100;
    final int maxCharOfObjects = 100;
    private static final Logger LOGGER = Logger.getLogger(MapStoreTest.class.getName());
    

    
    @Test 
    public void testMapStore() {

       MapStore<String, Object> store = new DotH22MapStoreFactory().newMapStore("testMap", null);
        
        
        
       assertTrue(store instanceof H22RegionMapStore);
        
       store.store(LONG_KEYNAME, CONTENT);
       store.store(KEYNAME, CONTENT);
       
       assertTrue(CONTENT.equals(store.load(KEYNAME)));
       assertTrue(CONTENT.equals(store.load(LONG_KEYNAME)));
       
       Iterable<String> iter = store.loadAllKeys();
       List<String> keys = new ArrayList<>();
       iter.forEach(keys::add);

       assertTrue(keys.contains(LONG_KEYNAME));
       assertTrue(keys.contains(KEYNAME));
        
    }
}
