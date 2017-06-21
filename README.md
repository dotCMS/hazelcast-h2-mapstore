# hazelcast-h2-mapstore
A H2 mapstore impl for hazelcast

To build:
`./gradlew shadowJar`

This will create a fat jar that can be added to a standalone hazel installation



To config:
```
    <map name="shorty">
        <map-store enabled="true" initial-mode="LAZY|EAGER">
          <factory-class-name>com.dotcms.hazelcast.mapstore.DotH22MapStoreFactory</factory-class-name>
        </map-store>
    
        <max-size>25000</max-size>
        <eviction-policy>LFU</eviction-policy>
    </map>

```

Where EAGER will load keys when the map is created and LAZY will just load them lazily
