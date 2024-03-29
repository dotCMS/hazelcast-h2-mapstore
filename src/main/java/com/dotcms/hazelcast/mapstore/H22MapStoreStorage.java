package com.dotcms.hazelcast.mapstore;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.comparator.LastModifiedFileComparator;
import org.apache.commons.io.filefilter.DirectoryFileFilter;

import com.google.common.io.Files;
import com.zaxxer.hikari.pool.HikariPool.PoolInitializationException;



public class H22MapStoreStorage {



    private Boolean isInitialized = false;

    final static String TABLE_PREFIX = "cache_table_";

    @SuppressWarnings("unchecked")
    private final static Map<Object, Object> DONT_CACHE_ME =
                    Collections.synchronizedMap(new LRUMap(1000));

    // number of different dbs to shard against
    private final int numberOfDbs =
                    HazelH2PropertyBundle.getIntProperty("mapstore.h22.number.of.dbs", 2);

    // number of tables in each db shard
    private final int numberOfTablesPerDb =
                    HazelH2PropertyBundle.getIntProperty("mapstore.h22.number.of.tables.per.db", 9);

    // limit error message to every 5 seconds;
    private final int limitErrorLogMillis = HazelH2PropertyBundle
                    .getIntProperty("mapstore.h22.limit.one.error.log.per.milliseconds", 5000);

    // create a new cache store if our errors are greater that this. Anything <1
    // will disable auto recover
    private final long recoverAfterErrors =
                    HazelH2PropertyBundle.getIntProperty("mapstore.h22.recover.after.errors", 5000);

    // try to recover with h2 if within this time (30m defualt)
    private final long recoverOnRestart = HazelH2PropertyBundle.getIntProperty(
                    "mapstore.h22.recover.if.restarted.in.milliseconds", 1000 * 60 * 30);
    private long lastLog = System.currentTimeMillis();
    private long[] errorCounter = new long[numberOfDbs];
    private final H22HikariPool[] pools = new H22HikariPool[numberOfDbs];
    private int failedFlushAlls = 0;
    private static final Logger logger = Logger.getLogger(H22MapStoreStorage.class.getName());
    final String dbRoot;


    public H22MapStoreStorage(final String dbRoot) {
        this.dbRoot = dbRoot;
    }


    public String getName() {
        return "H22 Map";
    }


    public String getKey() {
        return "H22Cache";
    }


    public boolean isDistributed() {
        return false;
    }


    public void init() throws Exception {

        // init the databases
        for (int i = 0; i < numberOfDbs; i++) {
            getPool(i, true);
        }
        isInitialized = true;

    }


    public boolean isInitialized() throws Exception {
        return isInitialized;
    }


    public void put(String group, String key, Object content) {

        // Building the key
        Fqn fqn = new Fqn(group, key);

        try {
            // Add the given content to the group and for a given key

            doUpsert(fqn, (Serializable) content);

        } catch (ClassCastException e) {
            DONT_CACHE_ME.put(key, fqn.toString());
            handleError(e, fqn);

        } catch (Exception e) {
            handleError(e, fqn);
        }
    }


    public Object get(String group, String key) {


        Object foundObject = null;
        long start = System.nanoTime();
        Fqn fqn = new Fqn(group, key);

        try {
            // Get the content from the group and for a given key;
            foundObject = doSelect(fqn);

        } catch (Exception e) {
            foundObject = null;
            handleError(e, fqn);
        }



        return foundObject;
    }


    public void remove(String groupName) {

        Fqn fqn = new Fqn(groupName);

        logger.info("Flushing H22 cache group:" + fqn
                        + " Note: this can be an expensive operation");

        try {
            for (int db = 0; db < numberOfDbs; db++) {

                for (int table = 0; table < numberOfTablesPerDb; table++) {
                    Connection connection = null;
                    PreparedStatement stmt = null;
                    final Optional<Connection> opt = createConnection(true, db);

                    if (!opt.isPresent()) {
                        throw new SQLException(
                                        "Unable to get connection when trying to remove in H22Cache");
                    }

                    try {
                        connection = opt.get();
                        logger.warning("connection.getAutoCommit():" + connection.getAutoCommit());
                        stmt = connection.prepareStatement("DELETE from " + TABLE_PREFIX + table
                                        + " WHERE cache_group = ?");
                        stmt.setString(1, fqn.group);
                        stmt.executeUpdate();
                    } finally {
                        try {

                            connection.close();
                        } catch (Exception e) {

                        }

                    }
                }



            }
        } catch (SQLException e) {

            handleError(e, fqn);
        }
    }


    public void remove(String group, String key) {
        Fqn fqn = new Fqn(group, key);
        try {

            if (key == null || key.length() == 0) {
                logger.warning("Empty key passed in, clearing group " + group + " by mistake");
            }

            // Invalidates from Cache a key from a given group
            doDelete(fqn);
        } catch (Exception e) {
            handleError(e, fqn);
        }
    }

    public void doTruncateTables() throws SQLException {

        for (int db = 0; db < numberOfDbs; db++) {
            Optional<H22HikariPool> poolOpt = getPool(db);
            if (!poolOpt.isPresent())
                continue;
            H22HikariPool pool = poolOpt.get();
            Optional<Connection> connOpt = pool.connection();
            if (!connOpt.isPresent())
                continue;
            Connection c = connOpt.get();
            try {

                for (int table = 0; table < numberOfTablesPerDb; table++) {
                    Statement stmt = c.createStatement();
                    stmt.execute("truncate table " + TABLE_PREFIX + table);
                    stmt.close();
                }
            } finally {

                c.close();
            }
        }

    }



    public void removeAll() {

        logger.info("Start Full Cache Flush in h22");
        long start = System.nanoTime();
        int failedThreshold = HazelH2PropertyBundle
                        .getIntProperty("mapstore.h22.rebuild.on.removeAll.failure.threshhold", 1);
        failedThreshold = (failedThreshold < 1) ? 1 : failedThreshold;
        // we either truncate the tables on a full flush or rebuild the tables
        if (HazelH2PropertyBundle.getBooleanProperty("mapstore.h22.rebuild.on.removeAll", false)
                        || failedFlushAlls == failedThreshold) {
            dispose(true);
        } else {
            try {
                doTruncateTables();
                failedFlushAlls = 0;
            } catch (SQLException e) {
                logger.warning(e.getMessage());
                failedFlushAlls++;
            }
        }

        if (failedFlushAlls == failedThreshold)


            DONT_CACHE_ME.clear();
        long end = System.nanoTime();
        logger.info("End Full Cache Flush in h22 : "
                        + TimeUnit.MILLISECONDS.convert(end - start, TimeUnit.NANOSECONDS) + "ms");

    }


    public Set<String> getGroups() {

        Set<String> groups = new HashSet<String>();
        try {
            for (int db = 0; db < numberOfDbs; db++) {
                Optional<Connection> opt = createConnection(true, db);
                if (!opt.isPresent()) {
                    continue;
                }
                Connection c = opt.get();
                for (int table = 0; table < numberOfTablesPerDb; table++) {
                    Statement stmt = c.createStatement();
                    ResultSet rs = stmt.executeQuery(
                                    "select DISTINCT(cache_group) from " + TABLE_PREFIX + table);
                    if (rs != null) {
                        while (rs.next()) {
                            String groupname = rs.getString(1);
                            if (groupname!=null && groupname.trim().length()>0) {
                                groups.add(groupname);
                            }
                        }
                        rs.close();
                        stmt.close();
                    }
                }
                c.close();
            }
        } catch (SQLException e) {
            logger.warning("cannot get groups : " + e.getMessage());
        }

        return groups;
    }



    public void shutdown() {
        isInitialized = false;
        // don't trash on shutdown
        dispose(false);
    }

    protected void dispose(boolean trashMe) {
        for (int db = 0; db < numberOfDbs; db++) {
            dispose(db, trashMe);
        }
    }

    protected void dispose(int db, boolean trashMe) {
        try {
            H22HikariPool pool = pools[db];
            pools[db] = null;
            if (pool != null) {
                pool.close();
                if (trashMe) {
                    final File trash = Files.createTempDir();
                    FileUtils.moveDirectory(new File(dbRoot + File.separator + db), trash);
                    Thread t = new Thread() {
                        public void run() {
                            logger.info("deleting: " + trash);
                            try {
                                FileUtils.deleteDirectory(trash);
                            } catch (IOException e) {
                                logger.severe(e.getMessage());
                                e.printStackTrace();
                            }
                        }
                    };
                    t.start();
                }

            }
        } catch (Exception e) {
            logger.severe(e.getMessage());
        }
    }


    private Optional<H22HikariPool> getPool(final int dbNum) throws SQLException {
        return getPool(dbNum, false);
    }

    private final Semaphore building = new Semaphore(1, true);

    private Optional<H22HikariPool> getPool(final int dbNum, final boolean startup)
                    throws SQLException {

        H22HikariPool source = pools[dbNum];
        if (source == null) {
            if (building.tryAcquire()) {
                Runnable creater = new Runnable() {

                    public void run() {
                        try {
                            logger.info("Initing H22 cache db:" + dbNum);
                            if (startup) {
                                pools[dbNum] = recoverLatestPool(dbNum);
                            } else {
                                pools[dbNum] = createPool(dbNum);
                            }
                        } catch (SQLException e) {
                            logger.severe(e.getMessage());
                        } finally {
                            building.release();
                            errorCounter[dbNum] = 0;
                        }
                    }
                };
                creater.run();
            }
            return Optional.empty();
        }

        return Optional.of(source);

    }

    private H22HikariPool createPool(int dbNum) throws SQLException {
        logger.info("Building new H22 Cache, db:" + dbNum);
        // create pool
        H22HikariPool source = new H22HikariPool(dbRoot, dbNum);
        // create table
        createTables(source);
        return source;
    }

    private H22HikariPool recoverLatestPool(int dbNum) throws SQLException {
        H22HikariPool source = null;
        File dbs = new File(dbRoot + File.separator + dbNum);
        if (dbs.exists() && dbs.isDirectory()) {
            File[] files = dbs.listFiles((FileFilter) DirectoryFileFilter.DIRECTORY);
            Arrays.sort(files, LastModifiedFileComparator.LASTMODIFIED_REVERSE);
            if (files.length > 0) {
                File myDb = files[0];
                if (files[0].isDirectory()) {
                    files = myDb.listFiles();
                    if (files.length > 0) {
                        Arrays.sort(files, LastModifiedFileComparator.LASTMODIFIED_REVERSE);
                        if (files[0].lastModified() + recoverOnRestart > System
                                        .currentTimeMillis()) {
                            logger.info("Recovering H22 Cache, db:" + dbNum + ":" + myDb.getName());
                            try {
                                source = new H22HikariPool(dbRoot, dbNum, myDb.getName());
                                createTables(source);
                                pools[dbNum] = source;
                            } catch (PoolInitializationException e) {
                                logger.warning("Failed to recover H2 Cache:" + e.getMessage());
                            }

                        }
                    }
                }
            }
        }
        if (source == null) {
            source = createPool(dbNum);
        }
        return source;
    }

    Optional<Connection> createConnection(boolean autoCommit, int dbnumber) throws SQLException {
        Optional<H22HikariPool> poolOpt = getPool(dbnumber);
        if (poolOpt.isPresent()) {
            Optional<Connection> opt = poolOpt.get().connection();
            if (opt.isPresent()) {
                if (autoCommit == false) {
                    opt.get().setAutoCommit(autoCommit);
                }
            }
            return opt;
        }
        return Optional.empty();
    }

    private boolean doUpsert(final Fqn fqn, final Serializable obj) throws Exception {
        long start = System.nanoTime();
        long bytes = 0;
        boolean worked = false;
        if (fqn == null || exclude(fqn)) {
            return worked;
        }

        Optional<Connection> opt = createConnection(true, db(fqn));
        if (!opt.isPresent()) {
            return worked;
        }
        Connection c = opt.get();



        String upsertSQL = "MERGE INTO `" + TABLE_PREFIX + table(fqn)
                        + "` key(cache_id) VALUES (?, ?, ?, ?)";

        PreparedStatement upsertStmt = null;
        try {
            upsertStmt = c.prepareStatement(upsertSQL);
            upsertStmt.setString(1, fqn.id);
            upsertStmt.setString(2, fqn.group);
            upsertStmt.setString(3, fqn.key);
            ObjectOutputStream output = null;
            OutputStream bout;
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            bout = new BufferedOutputStream(os, 8192);

            output = new ObjectOutputStream(bout);
            output.writeObject(obj);
            output.flush();
            byte[] data = os.toByteArray();
            bytes = data.length;
            upsertStmt.setBytes(4, data);

            worked = upsertStmt.execute();

        } finally {
            if (upsertStmt != null)
                upsertStmt.close();
            c.close();
        }


        return worked;
    }

    private Object doSelect(Fqn fqn) throws Exception {
        if (fqn == null || exclude(fqn)) {
            return null;
        }

        ObjectInputStream input = null;
        InputStream bin = null;
        InputStream is = null;
        Optional<Connection> opt = createConnection(true, db(fqn));
        if (!opt.isPresent()) {
            return null;
        }
        Connection c = opt.get();

        PreparedStatement stmt = null;
        try {

            stmt = c.prepareStatement("select CACHE_DATA from `" + TABLE_PREFIX + table(fqn)
                            + "` WHERE cache_id = ?");
            stmt.setString(1, fqn.id);
            ResultSet rs = stmt.executeQuery();
            if (!rs.next()) {
                return null;
            }
            is = new ByteArrayInputStream(rs.getBytes(1));
            bin = new BufferedInputStream(is, 8192);

            input = new ObjectInputStream(bin);
            return input.readObject();

        } finally {

            if (stmt != null)
                stmt.close();
            c.close();
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    logger.warning("should not be here:" + e.getMessage());
                }
            }
            if (bin != null) {
                try {
                    bin.close();
                } catch (IOException e) {
                    logger.warning("should not be here:" + e.getMessage());
                }
            }

            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    logger.warning("should not be here:" + e.getMessage());
                }
            }
        }
    }

    private void doDelete(Fqn fqn) throws SQLException {
        if (fqn == null) {
            return;
        }

        Optional<Connection> opt = createConnection(true, db(fqn));
        if (!opt.isPresent()) {
            return;
        }
        Connection c = opt.get();
        PreparedStatement pstmt = null;
        try {
            String sql = "DELETE from " + TABLE_PREFIX + table(fqn) + " WHERE cache_id = ?";
            pstmt = c.prepareStatement(sql);
            pstmt.setString(1, fqn.id);
            pstmt.execute();
            pstmt.close();
            c.close();
            DONT_CACHE_ME.remove(fqn.id);
        } finally {
            pstmt.close();
            c.close();
        }
    }

    private void createTables(H22HikariPool source) throws SQLException {
        Connection c = null;
        int i = 0;

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new SQLException("Unable to get connection", e);
        }
        i++;
        if (i == 100) {
            throw new SQLException("Unable to get connection");
        }

        Optional<Connection> opt = source.connection();
        c = opt.get();

        for (int table = 0; table < numberOfTablesPerDb; table++) {

            Statement s = c.createStatement();
            s.execute("CREATE CACHED TABLE IF NOT EXISTS `" + TABLE_PREFIX + table
                            + "` (cache_id bigint PRIMARY KEY, cache_group VARCHAR(255), cache_key VARCHAR(1000),  CACHE_DATA BLOB)");
            s.close();
            s = c.createStatement();
            s.execute("CREATE INDEX IF NOT EXISTS `idx_" + TABLE_PREFIX + table + "_index_` on "
                            + TABLE_PREFIX + table + "(cache_group)");
        }
        c.close();
    }


    public Set<String> getKeys(String groupName) {

        Set<String> keys = new HashSet<String>();
        int db = 0;
        Fqn fqn = new Fqn(groupName);
        try {
            for (db = 0; db < numberOfDbs; db++) {
                Optional<Connection> opt = createConnection(true, db);
                if (!opt.isPresent()) {
                    continue;
                }
                Connection c = opt.get();
                try {
                    for (int table = 0; table < numberOfTablesPerDb; table++) {
                        PreparedStatement stmt = c.prepareStatement("select cache_key from "
                                        + TABLE_PREFIX + table + " where cache_group = ?");
                        stmt.setString(1, fqn.group);
                        stmt.setFetchSize(1000);
                        ResultSet rs = stmt.executeQuery();
                        while (rs.next()) {
                            keys.add(rs.getString(1));
                        }
                        rs.close();
                    }
                } finally {
                    c.close();
                }
            }
        } catch (Exception ex) {
            handleError(ex, fqn);
        }

        return keys;
    }

    private void handleError(final Exception ex, final Fqn fqn) {
        // debug all errors
        logger.fine(ex.getMessage() + " on " + fqn);
        int db = db(fqn);
        if (lastLog + limitErrorLogMillis < System.currentTimeMillis()) {
            lastLog = System.currentTimeMillis();
            logger.warning("Error #" + errorCounter[db] + " " + ex.getMessage() + " on " + fqn);

        }

        errorCounter[db]++;
        if (errorCounter[db] > recoverAfterErrors && recoverAfterErrors > 0) {
            errorCounter[db] = 0;
            logger.severe("Errors exceeded " + recoverAfterErrors + " rebuilding H22 Cache for db"
                            + db);
            dispose(db, true);
        }

    }

    private String _getGroupCount(String groupName) throws SQLException {
        Fqn fqn = new Fqn(groupName);
        long ret = 0;
        for (int db = 0; db < numberOfDbs; db++) {
            Optional<Connection> opt = createConnection(true, db);
            if (!opt.isPresent()) {
                continue;
            }
            Connection c = opt.get();
            for (int table = 0; table < numberOfTablesPerDb; table++) {
                PreparedStatement stmt = c.prepareStatement("select count(*) from " + TABLE_PREFIX
                                + table + " where cache_group = ?");
                stmt.setString(1, fqn.group);
                ResultSet rs = stmt.executeQuery();
                if (rs != null) {
                    while (rs.next()) {
                        ret = ret + rs.getInt(1);
                    }
                    rs.close();
                    stmt.close();
                }
            }
            c.close();
        }
        return new Long(ret).toString();
    }

    private int db(Fqn fqn) {
        int hash = Math.abs(fqn.id.hashCode());
        return hash % numberOfDbs;
    }

    private int table(Fqn fqn) {
        int hash = Math.abs(fqn.id.hashCode());
        return hash % numberOfTablesPerDb;
    }

    /**
     * Method that verifies if must exclude content that can not or must not be added to this h2
     * cache based on the given cache group and key
     *
     * @return
     */
    private boolean exclude(Fqn fqn) {

        boolean exclude = DONT_CACHE_ME.containsKey(fqn.id);


        return exclude;
    }

}
