package com.dotcms.hazelcast.mapstore;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;
import java.util.logging.Logger;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;




public class H22HikariPool {

	final int dbNumber;
	final String dbRoot;
	final String database;
	final int maxPoolSize = HazelH2PropertyBundle.getIntProperty("mapstore.h22.db.poolsize.max", 500);
	final int connectionTimeout = HazelH2PropertyBundle.getIntProperty("mapstore.h22.db.connection.timeout", 1000);
	final int setLeakDetectionThreshold = HazelH2PropertyBundle.getIntProperty("mapstore.h22.db.leak.detection.timeout", 0);
	final HikariDataSource datasource;
	final String folderName;
	boolean running = false;
	final String extraParms = HazelH2PropertyBundle.getProperty("mapstore.h22.db.extra.params", ";MVCC=TRUE;DB_CLOSE_ON_EXIT=FALSE"); //;LOCK_MODE=0;DB_CLOSE_ON_EXIT=FALSE;FILE_LOCK=NO
	
	public H22HikariPool(String dbRoot, int dbNumber) {
		this(dbRoot,dbNumber,new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date()) );
	}
	
	public H22HikariPool(String dbRoot, int dbNumber, String database) {
		this.dbNumber = dbNumber;
		this.dbRoot = dbRoot;
		this.database = database;
		folderName = dbRoot  + File.separator  + dbNumber +File.separator 
				+ database;
		datasource = getDatasource();
		running = true;
	}
	

	public H22HikariPool(int dbNumber) {
		this(HazelH2PropertyBundle.getProperty("mapstore.h2.database.folder", "H22MapStore"), dbNumber);
	}

	private String getDbUrl() {
		String params = extraParms;
		new File(folderName).mkdirs();
		String ret = "jdbc:h2:" + folderName + File.separator + "cache" + params;
		return ret;
	}

	private HikariDataSource getDatasource() {

		HikariConfig config = new HikariConfig();
		config.setDataSourceClassName("org.h2.jdbcx.JdbcDataSource");
		config.setConnectionTestQuery("VALUES 1");
		config.addDataSourceProperty("URL", getDbUrl());
		config.addDataSourceProperty("user", "sa");
		config.addDataSourceProperty("password", "sa");
		config.setMaximumPoolSize(maxPoolSize);
		config.setConnectionTimeout(connectionTimeout);
        Logger logger = Logger.getLogger(this.getClass().getName());
        logger.info("H22 on disk cache:" + getDbUrl());
		if(setLeakDetectionThreshold>0){
			config.setLeakDetectionThreshold(setLeakDetectionThreshold);
		}
		return new HikariDataSource(config);

	}

	public boolean running() {
		return running;
	}

	public Optional<Connection> connection() throws SQLException {
		if (!running) {
			return Optional.empty();
		}

		return Optional.of(datasource.getConnection());
	}

	public void close() {
		running = false;
		datasource.close();
	}
	
	
	

}