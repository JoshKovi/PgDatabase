package com.kovisoft.pg.database.data.exports;

import com.kovisoft.pg.database.data.SQLRecord;
import com.kovisoft.simple.connection.pool.exports.ConnectionWrapper;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public abstract class DBManager implements AutoCloseable {

    // Method for table migrations
    abstract protected boolean migrateFromOldTables(List<AbstractMigration> tm, DBManagerConfig config);

    // Borrow Connection
    public abstract Connection borrowConnection(boolean isPrivileged) throws SQLException;
    public abstract ConnectionWrapper borrowCW(boolean isPrivileged) throws SQLException;

    abstract public Map<String, String> getPrepMap();
    abstract public List<Class<? extends SQLRecord>> getRecordClasses();

    //Primary DB build methods, this is more organizational help than part of the interface for the moment.
    abstract protected void createDBIfAbsent(String url, DBManagerConfig config, String dbName) throws SQLException, InterruptedException;
    abstract protected void addRole(String user, String pass) throws SQLException;

    abstract protected void createTablesFromRecords() throws SQLException;
    abstract protected void createTablesFromRecords(String user) throws SQLException;

    abstract protected void grantDefaultPrivledges(String user, String table) throws SQLException;
    abstract protected void grantPrivledgesToUser(String user, String table, String privledgeString) throws SQLException;

    abstract protected Map<String, List<String>> verifyColumnsMatch(Class<? extends SQLRecord> recordClass) throws SQLException;

    abstract protected void addMissingColumns(List<String> columns, Class<? extends SQLRecord> recordClass) throws SQLException;
    abstract protected void removeExtraColumns(List<String> columns, Class<? extends SQLRecord> recordClass) throws SQLException;
    abstract protected void adjustColumnsToMatch(Class<? extends SQLRecord> recordClass) throws SQLException;

}
