package com.kovisoft.pg.database.manager;

import com.kovisoft.logger.exports.Logger;
import com.kovisoft.logger.exports.LoggerFactory;
import com.kovisoft.pg.database.data.SQLRecord;
import com.kovisoft.pg.database.data.exports.*;
import com.kovisoft.pg.database.data.exports.SQLConvertType;
import com.kovisoft.pg.database.operations.DbOperationsAdminUser;
import com.kovisoft.pg.database.operations.AbstractDbOperations;
import com.kovisoft.pg.database.operations.DbOperationsBaseUser;
import com.kovisoft.simple.connection.pool.exports.ConnectionWrapper;
import com.kovisoft.simple.connection.pool.exports.PoolConfig;
import com.kovisoft.simple.connection.pool.exports.PoolFactory;
import com.kovisoft.simple.connection.pool.exports.SimplePgConnectionPool;

import java.io.IOException;
import java.lang.reflect.RecordComponent;
import java.sql.*;
import java.util.*;

public class DBManagerImpl extends DBManager {

    private ConnectionWrapper cwCurrent;
    private ConnectionWrapper cwArchived;
    private static final String F_URL = "jdbc:postgresql://%s:%d/";
    private List<Class<? extends SQLRecord>> recordClasses;
    private final Logger logger;
    private final HashMap<String, String> prepMap = new HashMap<>();
    private final HashMap<String, Integer> constMap = new HashMap<>();

    private SimplePgConnectionPool userConnectionPool;
    private SimplePgConnectionPool adminConnectionPool;
    private boolean isInInit = false;
    private boolean destructiveColumns = false;

    private static final int BORROW_ATTEMPTS = 5;
    private static final int TIMEOUT_BETWEEN_BORROWS = 10;

    //Column verification keys
    private static final String MISSING = "missing";
    private static final String EXIST_BUT_SHOULD_NOT = "additional";

    //Default SQL Strings
    private static final String TABLE_EXISTS = "SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_tables WHERE tablename = ?)";
    private static final String USER_EXISTS = "SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_user WHERE usename = ?)";
    private static final String GET_TABLE_COLUMNS = "SELECT column_name FROM information_schema.columns WHERE table_name = ?";

    private static final String REVOKE_ALL = "REVOKE ALL PRIVILEGES ON TABLE ";
    private static final String DEFAULT_PRIVILEGES = "GRANT SELECT ON TABLE ";
    private static final String DEFAULT_ADMIN_PRIVILEGES = "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE ";

    public DBManagerImpl(DBManagerConfig config) throws SQLException, InterruptedException {
        this(config, null);
    }

    public DBManagerImpl(DBManagerConfig config, List<AbstractMigration> tms) throws SQLException, InterruptedException {
        try{
            logger = LoggerFactory.createLogger(System.getProperty("user.dir") + "/logs",
                    "DB_Logger");
        } catch (IOException e) {
            throw new RuntimeException("Could not startup the handler logger!", e);
        }
        initDb(config, tms);
        String url = String.format(F_URL, config.getHost(), config.getPort()) + config.getDb();
        setupAdminConnectionPool(config, url);
        setupUserConnectionPool(config, url);
        // Should be fine without these but better safe than sorry.
        cwCurrent = null;
        isInInit = false;
    }

    private void setupUserConnectionPool(DBManagerConfig config, String url) throws SQLException {
        if(config.getUserThreads() == null){
            userConnectionPool = PoolFactory.createDefaultPgPool(url, config.getUser(), config.getPass());
        } else {
            PoolConfig pc = new PoolConfig(url, config.getUser(), config.getPass());
            pc.setMaxConnections(config.getUserThreads());
            userConnectionPool = PoolFactory.createPgPool(pc, prepMap, constMap);
        }
    }

    private void setupAdminConnectionPool(DBManagerConfig config, String url) throws SQLException {
        PoolConfig pc = new PoolConfig(url, config.getAdminUser(), config.getAdminPass());
        if(config.getAdminThreads() == null) pc.setMaxConnections(10);
        else pc.setMaxConnections(config.getAdminThreads());
        pc.setMinConnections(2);
        pc.setConnectionLifeSpan(60);
        pc.setConnectionCheckIntervals(5);
        pc.setRequestsPerMinutePerConn(10);
        adminConnectionPool = PoolFactory.createPgPool(pc, prepMap, constMap);
    }

    private void initDb(DBManagerConfig config, List<AbstractMigration> tms) throws SQLException, InterruptedException {
        this.isInInit = true;
        Map<Class<? extends SQLRecord>, String> records = config.getRecords();
        recordClasses = new ArrayList<>(records.keySet());
        String connString = String.format(F_URL, config.getHost(), config.getPort());
        if(config.getSuperUser() != null && config.getSuperPass() != null){

            if(config.isMigrate() && tms != null && !tms.isEmpty()){
                moveTables(tms, config);
            }

            // Create DB and then ConnectionWrapper
            createDBIfAbsent(connString + "postgres", config, config.getDb());
            cwCurrent = PoolFactory.createSingleConnectionWrapper(connString + config.getDb(),
                    config.getSuperUser(), config.getSuperPass());

            // Add rolls
            addRole(config.getAdminUser(), config.getAdminPass());
            addRole(config.getUser(), config.getPass());

            // Create Tables with default privileges for user
            destructiveColumns = config.isDestructiveColumns();

            createTablesFromRecords();
            // Grant privileges on all tables
            for(Class<? extends SQLRecord> recordClass : recordClasses){
                String recordString = recordClass.getSimpleName().toLowerCase();
                String privString = records.getOrDefault(recordClass, DEFAULT_PRIVILEGES);
                privString = (privString == null) ? DEFAULT_PRIVILEGES : privString;
                grantPrivledgesToUser(config.getUser(), recordString, privString);
                grantPrivledgesToUser(config.getAdminUser(), recordString, DEFAULT_ADMIN_PRIVILEGES);
            }

            // If necessary trigger migration of tables and data
            if(config.isMigrate() && tms != null && !tms.isEmpty()){
                if(!migrateFromOldTables(tms, config)) throw new RuntimeException("Migration failed during conversion of data");
            }
            try{
                cwCurrent.close();
                cwArchived.close();
            } catch (Exception e){
                logger.except("Exception occured while attempting to close init Connection Wrapper!", e);
            }
        } else {
            for(Class<? extends SQLRecord> recordClass : recordClasses){
                prepStatements(recordClass);
            }
        }

        isInInit = false;
    }

    /**
     * This is destructive move. If the tables are moved successfully the last action
     * is dropping all the tables in the database. This allows the tables to be created
     * in the event that a table columns type changed but the name did not.
     * If the migration fails this migration is in jeopardy so, you <u>SHOULD</u> exception out.
     *
     * @param tms    The table migrations to do, each migration should encompass the entire
     *               database for which the migration is occuring.
     * @param config The Database config
     * @throws RuntimeException Should not be caught, this should kill the application
     *                          as it's likely the migration of tables failed spectacularly... Sorry.
     */
    public void moveTables(List<AbstractMigration> tms, DBManagerConfig config) throws RuntimeException{
        String terminateConnectionsBase = "SELECT pg_terminate_backend(pg_stat_activity.pid)"
                + " FROM pg_stat_activity WHERE pg_stat_activity.datname = '";
        String connString = String.format(F_URL, config.getHost(), config.getPort());
        for(AbstractMigration tm : tms){
            String terminateArchive = terminateConnectionsBase + tm.getArchiveDB() + "';";
            String terminateCurrent = terminateConnectionsBase + tm.getCurrentDB() + "';";
            String dropArchive = "DROP DATABASE " + tm.getArchiveDB() + ";";
            String dbExists = "SELECT 1 FROM pg_database WHERE datname = '" + tm.getArchiveDB() + "';";
            String archiveCurrent = "CREATE DATABASE " + tm.getArchiveDB() + " WITH TEMPLATE " + tm.getCurrentDB() + " OWNER " + config.getAdminUser();
            String dropCurrentDatabase = "DROP DATABASE " + tm.getCurrentDB() + ";";

            try(Connection con = DriverManager.getConnection(connString + "postgres", config.getSuperUser(), config.getSuperPass())){
                // First section is backing up the database to an archive.
                Statement statement = con.createStatement();
                ResultSet rs = statement.executeQuery(dbExists);
                if(rs.next() && rs.getBoolean(1)){
                    statement.execute(terminateArchive);
                    statement.execute(dropArchive);
                }
                statement.execute(terminateCurrent);
                statement.execute(archiveCurrent);
                statement.execute(dropCurrentDatabase);
                createDBIfAbsent(connString + "postgres", config, tm.getCurrentDB());
                return;
            } catch (SQLException | InterruptedException e) {
                throw new RuntimeException("TABLE MOVES FAILED, DO NOT RUN AGAIN UNTIL YOU VERIFY ALL DATA IS IN THE PROPER LOCATION",e);
            }
        }
    }



    @Override
    protected boolean migrateFromOldTables(List<AbstractMigration> tms, DBManagerConfig config) {
        String connString = String.format(F_URL, config.getHost(), config.getPort());
        boolean withoutError = true;
        for(AbstractMigration tm : tms) {
            // First section is backing up the database to an archive.
            try {
                cwCurrent = PoolFactory.createSingleConnectionWrapper(connString + tm.getCurrentDB(),
                        config.getSuperUser(), config.getSuperPass());
                cwCurrent.addPreparedStatements(prepMap, constMap);

                cwArchived = PoolFactory.createSingleConnectionWrapper(connString + tm.getArchiveDB(),
                        config.getSuperUser(), config.getSuperPass());
                cwArchived.addPreparedStatements(prepMap, constMap);

                // The archive user only needs read access at this point so non-privileged use
                // is a great differentiator for the borrow connection calls.
                DBOperations current = new DbOperationsAdminUser(this);
                DBOperations archive = new DbOperationsBaseUser(this);

                Map<String, SQLRecord> migrationMap = tm.getMigrationMap();
                for(Map.Entry<String, SQLRecord> migrant : migrationMap.entrySet()){
                    String className = migrant.getValue().getClass().getSimpleName().toLowerCase();
                    // Check the amount of entries on the table prior to migrating any new data.
                    // It should be zero but check anyway.
                    List<? extends SQLRecord> newTableEntries = current.getAllEntries(migrant.getValue().getClass());
                    int newTableCount = (newTableEntries == null) ? 0 : newTableEntries.size();

                    //Get the data from the old table remove duplicates.
                    List<Map<String, Object>> oldTableEntries = archive.getAllEntriesAsMaps(migrant.getKey());
                    if(oldTableEntries == null || oldTableEntries.isEmpty()) continue;

                    // Old table has entries, convert them to objects (Checked for duplicates in convertEntries
                    List<? extends SQLRecord> converted =  convertEntries(oldTableEntries, migrant.getValue(), newTableEntries);

                    // Insert those new objects in new table
                    current.batchRequestsNoReturn(converted, prepMap.get(className + AbstractDbOperations.INSERT_MANY), false);

                    //Retrieve the new table with all entries
                    List<Map<String, Object>> getAllNewRecords = current.getAllEntriesAsMaps(className);

                    int totalEntries = (getAllNewRecords == null) ? 0 : getAllNewRecords.size();

                    //If the new table has all the expected values we should be good from an entry count standpoint
                    if(totalEntries - newTableCount == converted.size()){
                        logger.log("It appears that all entries are in order on the new table: "
                                + migrant.getValue().getClass().getSimpleName());
                    } else {
                        logger.warn("It appears that the number of entries added does not match the number of entries retrieved! table: "
                                + migrant.getValue().getClass().getSimpleName());
                        withoutError = false;
                    }
                }
            } catch (SQLException e) {
                // Take care to make sure this is not swallowed. This exception should exit the application.
                logger.except("Exception occurred during database backup operations... Bailing!", e);
                throw new RuntimeException(e);
            }
        }
        return withoutError;
    }

    // Inherited public Methods from AbstractDBManager

    @Override
    public Connection borrowConnection(boolean isPrivileged) throws SQLException {
        int count = 0;
        while(count < BORROW_ATTEMPTS){
            count++;
            try{
                if(isInInit && isPrivileged){
                    Connection conn = cwCurrent.borrowConnection();
                    cwCurrent.release();
                    return conn;
                }  else if (isInInit){
                    Connection conn = cwArchived.borrowConnection();
                    cwArchived.release();
                    return conn;
                } else if (isPrivileged){
                    return adminConnectionPool.borrowConnection().borrowConnection();
                } else {
                    return userConnectionPool.borrowConnection().borrowConnection();
                }
            } catch (SQLException | InterruptedException e){
                logger.except(String.format("Unable to borrow connection, Privileged Attempt: %b, Init: %b.", isPrivileged, isInInit), e);
                if(count == 4){
                    throw new SQLException("Unable to borrow connection from the pool!");
                }
            }
            try{
                Thread.sleep(TIMEOUT_BETWEEN_BORROWS);
            } catch (InterruptedException e){
                //I do not care.
            }
        }
        throw new SQLException("Unable to borrow connection from the pool!");
    }

    @Override
    public ConnectionWrapper borrowCW(boolean isPrivileged) throws SQLException {
        int count = 0;
        while(count < BORROW_ATTEMPTS){
            count++;
            try{
                if(isInInit && isPrivileged){
                    cwCurrent.release();
                    return cwCurrent;
                } else if (isInInit){
                    cwArchived.release();
                    return cwArchived;
                } else if (isPrivileged){
                    return adminConnectionPool.borrowConnection();
                } else {
                    return userConnectionPool.borrowConnection();
                }
            } catch (SQLException | InterruptedException e){
                logger.except(String.format("Unable to borrow wrapper, Privileged Attempt: %b, Init: %b.", isPrivileged, isInInit), e);
                if(count == 4){
                    throw new SQLException("Unable to borrow connection wrapper from the pool!");
                }
            }
            try{
                Thread.sleep(TIMEOUT_BETWEEN_BORROWS);
            } catch (InterruptedException e){
                //I do not care.
            }
        }
        throw new SQLException("Unable to borrow connection from the pool!");
    }

    @Override
    public Map<String, String> getPrepMap() {
        return prepMap;
    }

    @Override
    public List<Class<? extends SQLRecord>> getRecordClasses() {
        return new ArrayList<>(recordClasses);
    }


    // private and protected methods, some defined by DBManager some are just helpers.

    private Connection borrowConnection() throws SQLException {
        if(!isInInit) throw new SQLException("This method should only be used during initialization!");
        return borrowConnection(true);
    }

    @Override
    protected void createDBIfAbsent(String url, DBManagerConfig config, String dbName) throws SQLException, InterruptedException {
        try(Connection con = DriverManager.getConnection(url, config.getSuperUser(), config.getSuperPass());
            PreparedStatement stmt = con.prepareStatement("SELECT 1 FROM pg_database WHERE datname = ?");
            Statement cStmt = con.createStatement()){

            stmt.setString(1, dbName);
            ResultSet rs = stmt.executeQuery();
            if(rs.next()){logger.log("Database exists already!");}
            else{
                logger.log("Database needs to be created...");
                cStmt.executeUpdate("CREATE DATABASE " + dbName);
                Thread.sleep(3000); // Helps give some space for follow-up operations
            }
        }
    }

    @Override
    protected void addRole(String user, String pass) throws SQLException {
        if (!user.matches("[a-zA-Z0-9_]{1,30}")) {
            throw new IllegalArgumentException("Invalid username.");
        }
        if (!pass.matches("[\\S]{8,30}")) {
            throw new IllegalArgumentException("Invalid password.");
        }
        Connection connection = borrowConnection();
        try(PreparedStatement stmt = connection.prepareStatement("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname = ?)");
            PreparedStatement addStmt = connection.prepareStatement("CREATE ROLE " + user +" WITH LOGIN PASSWORD '" + pass + "'")){
            stmt.setString(1, user);
            ResultSet rs = stmt.executeQuery();
            if(rs.next() && rs.getBoolean(1)) logger.log("User role already exists.");
            else {
                addStmt.executeUpdate();

            }
        }
    }

    @Override
    protected void createTablesFromRecords() throws SQLException {
        for(Class<? extends SQLRecord> record : recordClasses){
            String createSQL = prepStatements(record);
            // If table exists make sure it has all columns, else create it.
            if(isValidTable(record.getSimpleName().toLowerCase())){
                Map<String, List<String>> columnMap = verifyColumnsMatch(record);
                addMissingColumns(columnMap.get(MISSING), record);
                if(destructiveColumns) removeExtraColumns(columnMap.get(EXIST_BUT_SHOULD_NOT), record);
            } else {
                try(PreparedStatement pStat = borrowConnection().prepareStatement(createSQL)){
                    pStat.executeUpdate();
                }
            }
        }
    }

    @Override
    protected void createTablesFromRecords(String user) throws SQLException {
        createTablesFromRecords();
        for(Class<? extends SQLRecord> record : recordClasses){
            grantDefaultPrivledges(user, record.getSimpleName().toLowerCase());
        }

    }

    @Override
    protected void grantDefaultPrivledges(String user, String table) throws SQLException {
        grantPrivledgesToUser(user, table, DEFAULT_PRIVILEGES);
    }

    @Override
    protected void grantPrivledgesToUser(String user, String table, String privledgeString) throws SQLException {
        if(!isValidRole(user) || !isValidTable(table) || privledgeString.contains(";")){
            throw new IllegalArgumentException("User or table name is invalid!");
        }
        String grantPriv = privledgeString + table +  " TO " + user + ";";
        try(PreparedStatement pStmt = borrowConnection().prepareStatement(grantPriv)){
            pStmt.executeUpdate();
        }
    }

    @Override
    protected Map<String, List<String>> verifyColumnsMatch(Class<? extends SQLRecord> recordClass) throws SQLException {
        List<String> recordColumns = new ArrayList<>(Arrays.stream(recordClass.getRecordComponents())
                .map(comp -> comp.getName().toLowerCase()).toList());
        Map<String, List<String>> map = new HashMap<>();
        try(PreparedStatement pStmt = borrowConnection().prepareStatement(GET_TABLE_COLUMNS)){
            pStmt.setString(1, recordClass.getSimpleName().toLowerCase());
            ResultSet rs = pStmt.executeQuery();
            List<String> dbColumns = new ArrayList<>();
            if(!rs.next()) {
                logger.error("There was no table to check the columns or the columns were empty!");
                return null;
            }
            do{
                dbColumns.add(rs.getString(1).toLowerCase());
            } while (rs.next());

            map.put(MISSING, recordColumns.stream().filter(entry -> !dbColumns.contains(entry)).toList());
            dbColumns.removeAll(recordColumns);
            map.put(EXIST_BUT_SHOULD_NOT, dbColumns);
        }
        return map;
    }

    @Override
    protected void addMissingColumns(List<String> columns, Class<? extends SQLRecord> recordClass) throws SQLException {
        StringBuilder addColumnsSB = new StringBuilder("ALTER TABLE ").append(recordClass.getSimpleName().toLowerCase());
        Map<String, SQLConvertType> typeMap = new HashMap<>();
        for(RecordComponent comp : recordClass.getRecordComponents()){
            if(columns.contains(comp.getName().toLowerCase())){
                typeMap.put(comp.getName().toLowerCase(), SQLConvertType.getByClassSimpleName(comp.getType().getSimpleName()));
            }
        }
        if(typeMap.size() == 0) return;
        for(Map.Entry<String, SQLConvertType> column : typeMap.entrySet()){
            if(column.getValue() == null) throw new IllegalArgumentException(
                    String.format("Could not alter %s with %s as it is an unsupported type!", recordClass.getSimpleName(),
                            column.getKey()));
            addColumnsSB.append(" ADD COLUMN ").append(column.getKey()).append(" ").append(column.getValue().SQL_TYPE).append(", ");
        }
        addColumnsSB.setLength(addColumnsSB.length() - 2);
        addColumnsSB.append(";");
        try(PreparedStatement pStmt = borrowConnection().prepareStatement(addColumnsSB.toString())){
            pStmt.executeUpdate();
        }
    }

    @Override
    protected void removeExtraColumns(List<String> columns, Class<? extends SQLRecord> recordClass) throws SQLException {
        StringBuilder removeColumnsSB = new StringBuilder("ALTER TABLE ").append(recordClass.getSimpleName().toLowerCase());
        Map<String, SQLConvertType> typeMap = new HashMap<>();
        for(RecordComponent comp : recordClass.getRecordComponents()){
            if(columns.contains(comp.getName().toLowerCase())){
                typeMap.put(comp.getName().toLowerCase(), SQLConvertType.getByClassSimpleName(comp.getType().getSimpleName()));
            }
        }
        if(typeMap.size() == 0) return;
        for(Map.Entry<String, SQLConvertType> column : typeMap.entrySet()){
            if(column.getValue() == null) throw new IllegalArgumentException(
                    String.format("Could not alter %s with %s as it is an unsupported type!", recordClass.getSimpleName(),
                            column.getKey()));
            removeColumnsSB.append(" DROP COLUMN ").append(column.getKey()).append(", ");
        }
        removeColumnsSB.setLength(removeColumnsSB.length() - 2);
        removeColumnsSB.append(";");
        try(PreparedStatement pStmt = borrowConnection().prepareStatement(removeColumnsSB.toString())){
            pStmt.executeUpdate();
        }
    }

    @Override
    protected void adjustColumnsToMatch(Class<? extends SQLRecord> recordClass) throws SQLException {
        Map<String, List<String>> columnModsMap = verifyColumnsMatch(recordClass);
        if(!columnModsMap.getOrDefault(MISSING, List.of()).isEmpty()) {
            addMissingColumns(columnModsMap.get(MISSING), recordClass);
        }
        if(!columnModsMap.getOrDefault(EXIST_BUT_SHOULD_NOT, List.of()).isEmpty()) {
            removeExtraColumns(columnModsMap.get(EXIST_BUT_SHOULD_NOT), recordClass);
        }
    }

    private List<? extends SQLRecord> convertEntries(List<Map<String, Object>> tableEntries, SQLRecord record, List<? extends SQLRecord> previousEntries){
        List<? extends SQLRecord> recordList = new ArrayList<>();
        List<FieldConverter> converter = record.getConverter();

        // Given the nature of SQLRecords getNewRecord method I don't actually need
        // to remove and change values, I just need to add another entry with the converted (optionally) obj
        for(Map<String, Object> entry : tableEntries){
            for(FieldConverter convert : converter){
                if(convert.newColumnName == null || convert.originalColumnName == null){
                    logger.warn(String.format("NewName or originalName is null! newColumnName = %s, originalColumnName = %s",
                            convert, convert.newColumnName, convert.originalColumnName));
                }
                try {
                    Object original = entry.get(convert.originalColumnName.toLowerCase());
                    entry.remove(convert.originalColumnName);
                    entry.put(convert.newColumnName, convert.typeConverter(original));
                } catch (Exception e){
                    logger.except("Exception occurred trying to convert value from " + convert.originalColumnName
                            + " to " + convert.newColumnName);
                    entry.put(convert.newColumnName, entry.get(convert.originalColumnName));
                }
            }

            //The risk here is in the unchecked cast typical of the creation of the object based off the map.
            //That being said it is a risk worth taking. Swallowing the exception is fine here after logging
            //as its possible that the other objects will succeed and only a handful need to have additional
            //considerations made.
            try{
                recordList.add(record.getNewRecord(entry));
            } catch (ClassCastException e){
                logger.except("Cast class exception caused during record creation!", e);
            }
        }

        if(previousEntries == null) return recordList;

        //This is costly, but it should ensure duplicates in db are made.
        Iterator<? extends SQLRecord> iter = recordList.iterator();
        while(iter.hasNext()){
            SQLRecord rec = iter.next();
            boolean isDuplicate = previousEntries.stream().anyMatch(rec::equalsWithoutId);
            if(isDuplicate) iter.remove();
        }
        return recordList;
    }




    private boolean isValidTable(String table) throws SQLException {
        try(PreparedStatement pStmt = borrowConnection().prepareStatement(TABLE_EXISTS)){
            pStmt.setString(1, table);
            ResultSet rs = pStmt.executeQuery();
            if(rs.next()){return rs.getBoolean(1);}
            return false;
        }

    }

    private boolean isValidRole(String user) throws SQLException {
        try(PreparedStatement pStmt = borrowConnection().prepareStatement(USER_EXISTS)){
            pStmt.setString(1, user);
            ResultSet rs = pStmt.executeQuery();
            if(rs.next()){return rs.getBoolean(1);}
            return false;
        }
    }


    /**
     * This method is long and almost grotesque but it should get the job done pretty well
     * Also most of the reason its long is also the reason it only loops once.
     * @param recordClass The SQLRecords class we are preparing statements for.
     * @return The string of the creation string, it should only be used once.
     * @throws SQLException If the Prepared statements fail this gets thrown.
     */
    private String prepStatements(Class<? extends SQLRecord> recordClass) throws SQLException {
        String tableName = recordClass.getSimpleName().toLowerCase();
        long start = System.currentTimeMillis();
        logger.info("Starting build of prepared statement strings for: " + tableName);
        RecordComponent[] comps = recordClass.getRecordComponents();
        if(comps == null){
            logger.error("Could not prepare statements for Record Class: " + recordClass + " as there are no components.");
            return null;
        }
        String primaryKey = comps[0].getName().toLowerCase();;
        StringBuilder createSB = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" ( ");
        if(!comps[0].getType().getSimpleName().equalsIgnoreCase("Long")){
            logger.error("This DBManager Implementation requires primary key to be a long and to be the first element in a record");
            throw new RuntimeException("Could not generate prepared statements for Record Class: " + recordClass);
        }
        createSB.append(primaryKey).append(" BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, ");
        StringBuilder insertSB = new StringBuilder("INSERT INTO ").append(tableName).append("(");
        StringBuilder insertValuesSB = new StringBuilder(") VALUES (");
        StringBuilder  updateSB = new StringBuilder("UPDATE ").append(tableName).append(" SET ");
        StringBuilder matchSB = new StringBuilder("SELECT * FROM ").append(tableName).append(" WHERE ");
        for(int i = 1; i < comps.length; i++){
            Class<?> classType = comps[i].getType();
            String fieldName = comps[i].getName().toLowerCase();
            //Table create
            String className = (classType.isEnum()) ? Integer.class.getSimpleName() : classType.getSimpleName();
            SQLConvertType sqlType = SQLConvertType.getByClassSimpleName(className);

            if(sqlType == null) throw new SQLException(String.format("Unsupported type of %s in table %s creation statement!",
                    className, tableName));
            boolean isJsonB = sqlType.isJsonb();

            createSB.append(fieldName).append(" ").append(sqlType.SQL_TYPE).append(", ");
            //Insert Record
            insertSB.append(fieldName).append(", ");
            if(isJsonB){
                insertValuesSB.append("?::jsonb,");
            } else {
                insertValuesSB.append("?, ");
            }
            //Update Record
            updateSB.append(fieldName).append(" = ?");
            if(isJsonB){
                updateSB.append("::JSONB, ");
            } else {
                updateSB.append(", ");
            }
            //Match Record
            matchSB.append(fieldName);
            if(isJsonB){
                matchSB.append("@> ?::JSONB AND ");
            } else {
                matchSB.append(" = ? AND ");
            }
        }

        insertSB.setLength(insertSB.length() - 2);
        insertSB.append(insertValuesSB);
        insertSB.setLength(insertSB.length() - 2);
        insertSB.append(")");
        prepMap.put(tableName + AbstractDbOperations.INSERT_MANY, insertSB.toString() +";");
        constMap.put(tableName + AbstractDbOperations.INSERT_MANY, Statement.RETURN_GENERATED_KEYS);
        prepMap.put(tableName + AbstractDbOperations.INSERT, insertSB.append(" RETURNING *;").toString());

        updateSB.setLength(updateSB.length() - 2);
        updateSB.append(" WHERE ").append(primaryKey).append(" = ?");
        prepMap.put(tableName + AbstractDbOperations.UPDATE_MANY, updateSB.toString() +";");
        prepMap.put(tableName + AbstractDbOperations.UPDATE, updateSB.append(" RETURNING *;").toString());

        matchSB.append(primaryKey).append(" = ?");
        prepMap.put(tableName + AbstractDbOperations.MATCH, matchSB.toString());

        //Get Record by ID
        prepMap.put(tableName + AbstractDbOperations.PRIMARY_KEY, "SELECT * FROM " + tableName + " WHERE " + primaryKey + " = ?" + ";");

        //Get All Records
        String allBase = "SELECT * FROM " + tableName;
        prepMap.put(tableName + AbstractDbOperations.ALL, allBase + ";");
        prepMap.put(tableName + AbstractDbOperations.ALL_LIMIT, allBase + " LIMIT ?;");
        prepMap.put(tableName + AbstractDbOperations.ALL_LIMIT_START, allBase + " OFFSET ? LIMIT ?;");
        prepMap.put(tableName + AbstractDbOperations.ALL_LIMIT_START_ORDER_DESC, allBase + " ORDER BY ? DESC OFFSET ? LIMIT ?;");
        prepMap.put(tableName + AbstractDbOperations.ALL_LIMIT_START_ORDER_ASC, allBase + " ORDER BY ? ASC OFFSET ? LIMIT ?;");

        //Delete Record by ID
        prepMap.put(tableName + AbstractDbOperations.DELETE, "DELETE FROM " + tableName + " WHERE " + primaryKey + " = ? RETURNING *" + ";");

        createSB.setLength(createSB.length() - 2);
        createSB.append(" );");



        logger.info("Completed build of prepared statement strings for: "
                + tableName + " took " + (System.currentTimeMillis() - start) + "ms");
        return createSB.toString();
    }

    @Override
    public void close() throws Exception {
        Exception lastException = null;
        try{
            userConnectionPool.shutDownPool();
        } catch (Exception e){
            logger.except("Exception when closing user Connection Pool!", e);
            lastException = e;
        }

        try{
            adminConnectionPool.shutDownPool();
        } catch (Exception e){
            logger.except("Exception when closing admin Connection Pool!", e);
            lastException = e;
        }
    }
}
