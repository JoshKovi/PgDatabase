package com.kovisoft.pg.database.data.exports;

import com.kovisoft.pg.database.data.SQLRecord;

import java.util.Map;
import java.util.TreeMap;


public class DBManagerConfig{

    private String superUser;
    private String superPass;
    private String user;
    private String pass;
    private String db;
    private String adminUser;
    private String adminPass;
    private String host;
    private int port;
    private boolean migrate;
    private boolean destructiveColumns;
    private Integer userThreads;
    private Integer adminThreads;
    private Map<Class<? extends SQLRecord>, String> records;

    /**
     * A DB Config that contains all the data that my implementation needs
     * @param superUser The database superuser for db wide changes, can be null.
     * @param superPass The database superuser password, can be null.
     * @param user The user which the connection will be maintained under after setup.
     * @param pass The user password for the connection.
     * @param db The database name
     * @param adminUser The admin user which the admin connection will be maintained under after setup.
     * @param adminPass The admin user password for the admin connection.
     * @param host The database host
     * @param port The database port
     * @param migrate If true in conjunction with passing TableMigration this will attempt to migrate the entire db.
     * @param destructiveColumns If true will delete columns that are not present in the records, use with caution.
     * @param userThreads The max datasource threads allocated for this user
     * @param adminThreads The max datasource threads allocated for this user
     * @param records The list of Records class that implement SQL Methods (each is its own table);
     */

    public DBManagerConfig(String superUser, String superPass, String user, String pass, String db,
                    String adminUser, String adminPass, String host, int port, boolean migrate,
                    boolean destructiveColumns, int userThreads, int adminThreads,
                           Map<Class<? extends SQLRecord>, String> records) {
        this.superUser = superUser;
        this.superPass = superPass;
        this.user = user;
        this.pass = pass;
        this.db = db;
        this.adminUser = adminUser;
        this.adminPass = adminPass;
        this.host = host;
        this.port = port;
        this.migrate = migrate;
        this.destructiveColumns = destructiveColumns;
        this.userThreads = userThreads;
        this.adminThreads = adminThreads;
        this.records = records;

    }

    @SuppressWarnings("unchecked")
    public DBManagerConfig(DBManagerConfig config, Map<String, Object> overrideMap) {
        TreeMap<String, Object> treeMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        treeMap.putAll(overrideMap);
        this.superUser = (String) overrideMap.getOrDefault("superUser", config.getSuperUser());
        this.superPass = (String) overrideMap.getOrDefault("superPass", config.getSuperPass());
        this.user = (String) overrideMap.getOrDefault("user", config.getUser());
        this.pass = (String) overrideMap.getOrDefault("pass", config.getPass());
        this.db = (String) overrideMap.getOrDefault("db", config.getDb());
        this.adminUser = (String) overrideMap.getOrDefault("adminUser", config.getAdminUser());
        this.adminPass = (String) overrideMap.getOrDefault("adminPass", config.getAdminPass());
        this.host = (String) overrideMap.getOrDefault("host", config.getHost());
        this.port = (int) overrideMap.getOrDefault("port", config.getPort());
        this.migrate = (boolean) overrideMap.getOrDefault("migrate", config.isMigrate());
        this.destructiveColumns = (boolean) overrideMap.getOrDefault("destructiveColumns", config.isDestructiveColumns());
        this.userThreads = (Integer) overrideMap.getOrDefault("userThreads", config.getUserThreads());
        this.adminThreads = (Integer) overrideMap.getOrDefault("adminThreads", config.getAdminThreads());
        this.records = (Map<Class<? extends SQLRecord>, String>) treeMap.getOrDefault("records", config.getRecords());
    }

    public DBManagerConfig(Map<String, Object> creationMap) throws ClassCastException {
        TreeMap<String, Object> treeMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        treeMap.putAll(creationMap);
        this.superUser = (String) treeMap.getOrDefault("superUser", null);
        this.superPass = (String) treeMap.getOrDefault("superPass", null);
        this.user = (String) treeMap.getOrDefault("user", null);
        this.pass = (String) treeMap.getOrDefault("pass", null);
        this.db = (String) treeMap.getOrDefault("db", null);
        this.adminUser = (String) treeMap.getOrDefault("adminUser", null);
        this.adminPass = (String) treeMap.getOrDefault("adminPass", null);
        this.host = (String) treeMap.getOrDefault("host", null);
        this.port = (int) treeMap.getOrDefault("port", null);
        this.migrate = (boolean) treeMap.getOrDefault("migrate", false);
        this.destructiveColumns = (boolean) treeMap.getOrDefault("destructiveColumns", false);
        this.userThreads = (Integer) treeMap.getOrDefault("userThreads", null);
        this.adminThreads = (Integer) treeMap.getOrDefault("adminThreads", null);
        this.records = (Map<Class<? extends SQLRecord>, String>) treeMap.getOrDefault("records", null);
    }

    public String getSuperUser() {
        return superUser.toLowerCase();
    }

    public void setSuperUser(String superUser) {
        this.superUser = superUser;
    }

    public String getSuperPass() {
        return superPass;
    }

    public void setSuperPass(String superPass) {
        this.superPass = superPass;
    }

    public String getUser() {
        return user.toLowerCase();
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPass() {
        return pass;
    }

    public void setPass(String pass) {
        this.pass = pass;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getAdminUser() {
        return adminUser.toLowerCase();
    }

    public void setAdminUser(String adminUser) {
        this.adminUser = adminUser;
    }

    public String getAdminPass() {
        return adminPass;
    }

    public void setAdminPass(String adminPass) {
        this.adminPass = adminPass;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public boolean isMigrate() {
        return migrate;
    }

    public void setMigrate(boolean migrate) {
        this.migrate = migrate;
    }

    public boolean isDestructiveColumns() {
        return destructiveColumns;
    }

    public void setDestructiveColumns(boolean destructiveColumns) {
        this.destructiveColumns = destructiveColumns;
    }

    public Integer getUserThreads() {
        return userThreads;
    }

    public void setUserThreads(Integer userThreads) {
        this.userThreads = userThreads;
    }

    public Integer getAdminThreads() {
        return adminThreads;
    }

    public void setAdminThreads(Integer adminThreads) {
        this.adminThreads = adminThreads;
    }

    public Map<Class<? extends SQLRecord>, String> getRecords() {
        return records;
    }

    public void setRecords(Map<Class<? extends SQLRecord>, String> records) {
        this.records = records;
    }


}