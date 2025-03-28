package com.kovisoft.pg.database.operations;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.kovisoft.logger.exports.Logger;
import com.kovisoft.logger.exports.LoggerFactory;
import com.kovisoft.pg.database.data.CompoundSQLRecord;
import com.kovisoft.pg.database.data.CompoundSQLRecordClass;
import com.kovisoft.pg.database.data.SQLRecord;
import com.kovisoft.pg.database.data.exports.*;
import com.kovisoft.simple.connection.pool.exports.ConnectionWrapper;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.RecordComponent;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class DbOperationsBaseUser extends AbstractDbOperations implements DBOperations {

    protected final Logger logger;
    protected DBManager dbManager;
    protected ObjectMapper om = new ObjectMapper();

    public DbOperationsBaseUser(){
        try{
            logger = LoggerFactory.createLogger(System.getProperty("user.dir") + "/logs",
                    "DB_Logger");
        } catch (IOException e) {
            throw new RuntimeException("Could not startup the DB_Logger logger!", e);
        }
    }

    public DbOperationsBaseUser(DBManager dbManager){
        try{
            logger = LoggerFactory.createLogger(System.getProperty("user.dir") + "/logs",
                    "DB_Logger");
        } catch (IOException e) {
            throw new RuntimeException("Could not startup the DB_Logger logger!", e);
        }
        this.dbManager = dbManager;
    }

    protected Connection borrowConnection() throws SQLException {
        return dbManager.borrowConnection(false);
    }

    protected ConnectionWrapper borrowCW() throws SQLException {
        return dbManager.borrowCW(false);
    }

    public void swapOutDBManager(DBManager dbManager){
        this.dbManager = dbManager;
    }


    @Override
    public <T extends SQLRecord> T addRecord(T record) {
        try{

            PreparedStatement pStmt = borrowCW().getPreparedStatement(record.getTableName() + INSERT);
            populateStatement(pStmt, record, false);
            return record.getNewRecord(executeSingleQuery(pStmt));
        } catch (Exception e) {
            logger.except("Exception occured during add single record event!", e);
        }
        return null;
    }

    @Override
    public <T extends SQLRecord> List<T> addRecords(List<T> records) {
        try{
            ConnectionWrapper cw = borrowCW();
            Connection connection = cw.borrowConnection();
            connection.setAutoCommit(false);
            Map<Class<T>, PreparedStatement> batchMap = batchRequests(records, INSERT_MANY, false, cw);
            connection.commit();
            connection.setAutoCommit(true);
            for(Map.Entry<Class<T>, PreparedStatement> entry : batchMap.entrySet()){
                try{
                    ResultSet rs = entry.getValue().getGeneratedKeys();
                    if(!rs.next()) throw new SQLException("Unable to retrieve inserted primaryKeys from execution on " + entry.getKey().getSimpleName());
                    List<Long> generatedIds = new ArrayList<>();
                    do{
                        generatedIds.add(rs.getLong(1));
                    } while(rs.next());
                    return getRecordsByIds(generatedIds, entry.getKey());
                } catch (Exception e){
                    logger.except("Exception occurred during retrieval of generated primaryKeys for: " + entry.getKey().getSimpleName(), e);
                }
            }
        } catch (SQLException e) {
            logger.except("Exception occurred during overall add records batching process", e);
        }
        return List.of();
    }

    @Override
    public <T extends CompoundSQLRecordClass> T addCompoundRecord(T record) {
        try{
            List<? extends SQLRecord> children = record.getChildRecords();
            SQLRecord parent = record.getParentRecord();
            parent = updateOrAddRecord(parent);
            children = updateAndAddRecords(children);
            record.setParentRecord(parent);
            record.setChildRecords(children);
            SQLRecord cRecord = record.getCompoundRecord();
            SQLRecord match = getMatchNoId(cRecord);
            if(match == null){
                match = updateOrAddRecord(cRecord);
            }
            record.setCompoundRecord(match);
            return record;
        } catch (Exception e){
            logger.except("Exception occurred during add of Compound Record.", e);
        }
        return null;
    }

    @Override
    public <T extends CompoundSQLRecordClass> List<T> addCompoundRecords(List<T> records) {
        //TODO Fix this when I fix the batching tables stuff.
        if(records == null || records.isEmpty()) return records;
        List<T> returnedRecords = new ArrayList<>(records.size());
        for(T record : records){
            CompoundSQLRecordClass recordInstance = addCompoundRecord(record);
            if(recordInstance == null) continue;
            returnedRecords.add((T)recordInstance);
        }
        return returnedRecords;
    }


    @Override
    public <T extends SQLRecord> T updateRecord(T record) {
        try{
            PreparedStatement pStmt = borrowCW().getPreparedStatement(record.getTableName() + UPDATE);
            populateStatement(pStmt, record, true);
            return record.getNewRecord(executeSingleQuery(pStmt));
        } catch (Exception e) {
            logger.except("Exception occured during add single record event!", e);
        }
        return null;
    }

    @Override
    public <T extends SQLRecord> List<T> updateRecords(List<T> records) {
        try{
            ConnectionWrapper cw = borrowCW();
            Connection connection = cw.borrowConnection();
            connection.setAutoCommit(false);
            Map<Class<T>, PreparedStatement> batchMap = batchRequests(records, UPDATE_MANY, true, cw);
            connection.commit();
            connection.setAutoCommit(true);
            for(Map.Entry<Class<T>, PreparedStatement> entry : batchMap.entrySet()){
                try{
                    return getRecordsByIds(records.stream().map(SQLRecord::getPrimaryKey).toList(), (Class<T>) records.getFirst().getClass());
                } catch (Exception e){
                    logger.except("Exception occurred during retrieval of generated primaryKeys for: " + entry.getKey().getSimpleName(), e);
                }
            }
        } catch (SQLException e) {
            logger.except("Exception occurred during overall add records batching process", e);
        }
        return List.of();
    }

    @Override
    public <T extends CompoundSQLRecordClass> T updateCompoundRecord(T record) {
        // TODO: For now there is no difference
        return addCompoundRecord(record);
    }

    @Override
    public <T extends CompoundSQLRecordClass> List<T> updateCompoundRecords(List<T> records) {
        // TODO: For now there is no difference
        return addCompoundRecords(records);
    }

    @Override
    public <T extends SQLRecord> T updateOrAddRecord(T record) {
        return ((record.getPrimaryKey() == null) ? addRecord(record) : updateRecord(record));
    }

    @Override
    public <T extends SQLRecord> List<T> updateAndAddRecords(List<T> records) {
        List<T> inserts = records.stream().filter(record -> record.getPrimaryKey() == null).toList();
        List<T> updates = records.stream().filter(record -> record.getPrimaryKey() != null).toList();
        List<T> returns = new ArrayList<>(records.size());

        // This should appropriately group inserts and adds so that multiple classes in a single list
        // work correctly by adding them in "batches"... Kind of a stop-gap until I spend the time
        // differentiating in batching, and creating.
        inserts.stream().collect(Collectors.groupingBy(SQLRecord::getClass))
                .forEach((clazz, groupRecords) -> {
                    returns.addAll(addRecords(groupRecords));
                });
        updates.stream().collect(Collectors.groupingBy(SQLRecord::getClass))
                .forEach((clazz, groupRecords) -> {
                    returns.addAll(updateRecords(groupRecords));
                });
//        returns.addAll(addRecords(inserts));
//        returns.addAll(updateRecords(updates));
        return returns;
    }

    @Override
    public <T extends CompoundSQLRecordClass> T updateOrAddCompoundRecord(T record) {
        // TODO: For now there is no difference
        return addCompoundRecord(record);
    }

    @Override
    public <T extends CompoundSQLRecordClass> List<T> updateOrAddCompoundRecords(List<T> records) {
        // TODO: For now there is no difference
        return addCompoundRecords(records);
    }

    @Override
    public <T extends SQLRecord> T getMatch(T record) {
        try {
            PreparedStatement pStmt = borrowCW().getPreparedStatement(record.getTableName() + MATCH);
            populateStatement(pStmt, record, true); //Bit hacky, but it works so... not a hack ;)
            return record.getNewRecord(executeSingleQuery(pStmt));
        } catch (Exception e) {
            logger.except("Match attempt failed with exception", e);
        }
        return null;
    }

    @Override
    public <T extends SQLRecord> T getMatchNoId(T record){
        try{
            PreparedStatement pStmt = borrowCW().getPreparedStatement(record.getTableName() + MATCH_NO_ID);
            populateStatement(pStmt, record, false);
            return record.getNewRecord(executeSingleQuery(pStmt));
        } catch (Exception e) {
            logger.except("Match without id attempt failed with exception", e);
        }
        return null;
    }

    @Override
    public <T extends CompoundSQLRecordClass> T getCompoundMatch(T record) {
        return null;
    }

    @Override
    public <T extends SQLRecord> T  getRecordById(Long primaryKey, T record) {
        try {
            PreparedStatement pStmt = borrowCW().getPreparedStatement(record.getTableName() + PRIMARY_KEY);
            pStmt.setLong(1, primaryKey);
            return record.getNewRecord(executeSingleQuery(pStmt));
        } catch (Exception e){
            logger.except("Get record by primaryKey failed with exception", e);
        }
        return null;
    }

    @Override
    public <T extends SQLRecord> T  getRecordById(Long primaryKey, Class<T> recordClass) {
        try {
            PreparedStatement pStmt = borrowCW().getPreparedStatement(recordClass.getSimpleName().toLowerCase() + PRIMARY_KEY);
            pStmt.setLong(1, primaryKey);
            Map<String, Object> objMap = executeSingleQuery(pStmt);
            return reflectRecordFromMap(primaryKey, objMap, recordClass);
        } catch (Exception e){
            logger.except("Get record by primaryKey failed with exception", e);
        }
        return null;
    }

    @Override
    public <T extends SQLRecord> T  getRecordById(Long primaryKey, String tableName) {
        try {
            PreparedStatement pStmt = borrowCW().getPreparedStatement(tableName.toLowerCase() + PRIMARY_KEY);
            pStmt.setLong(1, primaryKey);
            Map<String, Object> objMap = executeSingleQuery(pStmt);
            Optional<Class<? extends SQLRecord>> recordClass = dbManager.getRecordClasses().stream()
                    .filter(clazz -> clazz.getSimpleName().equalsIgnoreCase(tableName)).findFirst();
            if(recordClass.isEmpty()) throw new IllegalArgumentException("No class matches the table! " + tableName);
            return reflectRecordFromMap(primaryKey, objMap, (Class<T>) recordClass.get());
        } catch (Exception e){
            logger.except("Get record by primaryKey failed with exception", e);
        }
        return null;
    }

    @Override
    public <T extends SQLRecord> List<T> getRecordsByIds(List<Long> primaryKeys, Class<T> recordClass) {
        List<Long> nonNullPrimaryKeys = primaryKeys.stream().filter(Objects::nonNull).toList();
        StringBuilder sb = new StringBuilder("SELECT * FROM ").append(recordClass.getSimpleName().toLowerCase());
        String primaryKeyName = recordClass.getRecordComponents()[0].getName().toLowerCase();;
        sb.append(" WHERE ").append(primaryKeyName).append(" IN (")
                .append(String.join(",", Collections.nCopies(nonNullPrimaryKeys.size(), "?"))).append(");");
        try(PreparedStatement pStmt = borrowConnection().prepareStatement(sb.toString())){
            for(int i = 0; i < nonNullPrimaryKeys.size(); i++){
                pStmt.setLong(i+1, nonNullPrimaryKeys.get(i));
            }
            ResultSet rs = pStmt.executeQuery();
            List<Map<String, Object>> objMaps = processQueryResultSet(rs);
            if(objMaps == null){
                logger.error("Really not one correct primaryKey? You fucking donkey!");
                return List.of();
            }

            return reflectRecordsFromMaps(objMaps, recordClass);
        } catch (Exception e) {
            logger.except("Failed to get records by primaryKeys for Table: " + recordClass.getSimpleName(), e);
        }
        return List.of();
    }

    @Override
    public <T extends CompoundSQLRecordClass> T getCompoundRecordById(Long primaryKey, T recordInstance) {
        try {
            Class<?> compoundClass = recordInstance.getCompoundClass();
            CompoundSQLRecord cRec = (CompoundSQLRecord) compoundClass.getDeclaredConstructor().newInstance();
            cRec = getRecordById(primaryKey, cRec);
            if(cRec.getChildKeys().size() != cRec.getChildTables().size()){
                throw new IllegalStateException("The Compound Record Table and Key lists do not match in size!");
            }
            SQLRecord parent = getRecordById(cRec.getParentKey(), cRec.getParentTable());

            List<? extends SQLRecord> childRecords = new ArrayList<>();
            for(int i = 0; i < cRec.getChildTables().size(); i++){
                childRecords.add(getRecordById(cRec.getChildKeys().get(i), cRec.getChildTables().get(i)));
            }
            Constructor<T> classConstructor = (Constructor<T>) recordInstance.getClass().getDeclaredConstructor(SQLRecord.class, List.class, CompoundSQLRecord.class);
            return classConstructor.newInstance(parent, childRecords, cRec);

        } catch (Exception e){
            logger.except("Exception occurred while attempting to build Compound Record Class!", e);
        }
        return null;
    }
//
//    @Override
//    public <T extends CompoundSQLRecordClass> T getCompoundRecordById(Long id, Class<T> recordClass) {
//        return null;
//    }
//
//    @Override
//    public <T extends CompoundSQLRecordClass> T getCompoundRecordById(Long id, String tableName) {
//        return null;
//    }
//
//    @Override
//    public <T extends CompoundSQLRecordClass> List<T> getCompoundRecordsById(List<Long> ids, Class<T> recordClass) {
//        return null;
//    }

    //TODO: Implement these later when I need them, for now they are placeholders.
    @Override
    public <T extends SQLRecord> List<T> getMatchByColumnNames(T record, List<String> columnNames) {
        Class<?> clazz = record.getClass();
        StringBuilder sb = new StringBuilder("SELECT * FROM ");
        sb.append(clazz.getSimpleName().toLowerCase()).append(" WHERE ");
        RecordComponent[] comps = clazz.getRecordComponents();
        try{
            List<Object> objList = new ArrayList<>();
            for(RecordComponent comp : comps){
                String fieldName = comp.getName();
                if(!columnNames.contains(fieldName)) continue;
                Object fieldValue = record.getObjectValueByFieldName(fieldName);
                if(fieldValue != null) {
                    objList.add(fieldValue);
                    sb.append(fieldName.toLowerCase());
                    SQLConvertType sqlConvertType = SQLConvertType.getByClassSimpleName(fieldValue.getClass().getSimpleName(), true);
                    if(sqlConvertType.isJsonb()){
                        sb.append(" @> ?::JSONB AND ");
                    } else {
                        sb.append(" = ? AND ");
                    }
                }

            }
            sb.setLength(sb.length() - 4);
            PreparedStatement pStmt = borrowCW().getPreparedStatement(sb.toString());
            for(int i = 0; i < objList.size(); i++){
                Class<?> objClass = objList.get(i).getClass();
                if(objClass == LocalDateTime.class){
                    pStmt.setString(i+1, objList.get(i).toString());
                } else if(objClass.isEnum()){
                    pStmt.setInt(i, ((Enum<?>)objList.get(i)).ordinal());
                } else if(objClass.equals(ArrayListHolder.class)){
                    ArrayListHolder<?> arrayListHolder = ((ArrayListHolder<?>)objList.get(i));
                    String jsonB = om.writeValueAsString(arrayListHolder);
                    pStmt.setString(i, jsonB);
                } else if(objClass.equals(HashMapHolder.class)){
                    HashMapHolder<?, ?> hashMapHolder = ((HashMapHolder<?, ?>)objList.get(i));
                    String jsonB = om.writeValueAsString(hashMapHolder);
                    pStmt.setString(i, jsonB);
                } else {
                    pStmt.setObject(i+1, objList.get(i));
                }
            }
            ResultSet rs = pStmt.executeQuery();
            List<? extends SQLRecord> matches = reflectRecordsFromMaps(processQueryResultSet(rs), record.getClass());
            pStmt.close();
            return (matches != null) ? (List<T>) matches : List.of();
        } catch (Exception e) {
            logger.except("Unable to match record by column names.", e);
        }
        return List.of();
    }

    @Override
    public <T extends SQLRecord> List<T> getMatchByComponents(T record, RecordComponent[] components) {
        throw new RuntimeException("This method is not currently implemented");
    }

    @Override
    public <T extends SQLRecord> List<T> getAllEntries(T record) {
        try{
            PreparedStatement pStmt = borrowCW().getPreparedStatement(record.getTableName() + ALL);
            ResultSet rs = pStmt.executeQuery();
            List<Map<String, Object>> objMaps = processQueryResultSet(rs);
            return reflectRecordsFromMaps(objMaps, record.getClass());
        } catch (Exception e) {
            logger.except("Failed to retrieve all records from table: " + record.getClass().getSimpleName(), e);
        }
        return List.of();
    }

    @Override
    public <T extends SQLRecord> List<T> getAllEntries(Class<T> recordClass) {
        try{
            PreparedStatement pStmt = borrowCW().getPreparedStatement(recordClass.getSimpleName().toLowerCase() + ALL);
            ResultSet rs = pStmt.executeQuery();
            List<Map<String, Object>> objMaps = processQueryResultSet(rs);
            return reflectRecordsFromMaps(objMaps, recordClass);
        } catch (Exception e) {
            logger.except("Failed to retrieve all records from table: " + recordClass.getSimpleName(), e);
        }
        return List.of();
    }

    @Override
    public <T extends SQLRecord> List<T> getAllEntries(Class<T> recordClass, int limit) {
        try{
            PreparedStatement pStmt = borrowCW().getPreparedStatement(recordClass.getSimpleName().toLowerCase() + ALL_LIMIT);
            pStmt.setInt(1, limit);
            ResultSet rs = pStmt.executeQuery();
            List<Map<String, Object>> objMaps = processQueryResultSet(rs);
            return reflectRecordsFromMaps(objMaps, recordClass);
        } catch (Exception e) {
            logger.except("Failed to retrieve all records from table: " + recordClass.getSimpleName(), e);
        }
        return List.of();
    }

    @Override
    public <T extends SQLRecord> List<T> getAllEntries(Class<T> recordClass, int limit, int startIndex) {
        try{
            PreparedStatement pStmt = borrowCW().getPreparedStatement(recordClass.getSimpleName().toLowerCase() + ALL_LIMIT_START);
            pStmt.setInt(1, startIndex);
            pStmt.setInt(2, limit);
            ResultSet rs = pStmt.executeQuery();
            List<Map<String, Object>> objMaps = processQueryResultSet(rs);
            return reflectRecordsFromMaps(objMaps, recordClass);
        } catch (Exception e) {
            logger.except("Failed to retrieve all records from table: " + recordClass.getSimpleName(), e);
        }
        return List.of();
    }

    @Override
    public <T extends SQLRecord> List<T> getAllEntriesAscending(Class<T> recordClass, int limit, int startIndex, String columnName) {
        try{
            PreparedStatement pStmt = borrowCW().getPreparedStatement(recordClass.getSimpleName().toLowerCase() + ALL_LIMIT_START_ORDER_ASC);
            pStmt.setString(1, columnName);
            pStmt.setInt(2, startIndex);
            pStmt.setInt(3, limit);
            ResultSet rs = pStmt.executeQuery();
            List<Map<String, Object>> objMaps = processQueryResultSet(rs);
            return reflectRecordsFromMaps(objMaps, recordClass);
        } catch (Exception e) {
            logger.except("Failed to retrieve all records from table: " + recordClass.getSimpleName(), e);
        }
        return List.of();
    }

    @Override
    public <T extends SQLRecord> List<T> getAllEntriesDescending(Class<T> recordClass, int limit, int startIndex, String columnName) {
        try{
            PreparedStatement pStmt = borrowCW().getPreparedStatement(recordClass.getSimpleName().toLowerCase() + ALL_LIMIT_START_ORDER_DESC);
            pStmt.setString(1, columnName);
            pStmt.setInt(2, startIndex);
            pStmt.setInt(3, limit);
            ResultSet rs = pStmt.executeQuery();
            List<Map<String, Object>> objMaps = processQueryResultSet(rs);
            return reflectRecordsFromMaps(objMaps, recordClass);
        } catch (Exception e) {
            logger.except("Failed to retrieve all records from table: " + recordClass.getSimpleName(), e);
        }
        return List.of();
    }


    @Override
    public List<Map<String, Object>> getAllEntriesAsMaps(String tableName) {
        logger.warn(String.format("Doing a migration retrieve for %s? Otherwise this is very peculiar", tableName));
        try (PreparedStatement pStmt = borrowConnection().prepareStatement("SELECT * FROM " + tableName + ";")){
            ResultSet rs = pStmt.executeQuery();
            return processQueryResultSet(rs);
        } catch (Exception e) {
            logger.except("Failed to retrieve all records from table: " + tableName, e);
        }
        return null;
    }

    @Override
    public <T extends SQLRecord> T deleteById(T record) {
        if(record.getPrimaryKey() == null){
            logger.error("Passed record had a null primaryKey, we cannot delete that which does not exist.");
            return null;
        }
        try{
            PreparedStatement pStmt = borrowCW().getPreparedStatement(record.getTableName() + DELETE);
            pStmt.setLong(1, record.getPrimaryKey());
            return record.getNewRecord(executeSingleQuery(pStmt));
        } catch (Exception e) {
            logger.except(String.format("Failed to delete record %d from table: %s", record.getPrimaryKey(), record.getClass().getSimpleName()), e);
        }
        return null;
    }

    @Override
    public <T extends SQLRecord> T deleteById(Long primaryKey, Class<T> recordClass) {
        if(primaryKey == null){
            logger.error("Passed a null primaryKey, we cannot delete that which does not exist.");
            return null;
        }
        try{
            PreparedStatement pStmt = borrowCW().getPreparedStatement(recordClass.getSimpleName().toLowerCase() + DELETE);
            pStmt.setLong(1, primaryKey);
            return reflectRecordFromMap(primaryKey, executeSingleQuery(pStmt), recordClass);
        } catch (Exception e) {
            logger.except(String.format("Failed to delete record %d from table: %s", primaryKey, recordClass.getSimpleName()), e);
        }
        return null;
    }

    @Override// Only a single table!
    public <T extends SQLRecord> List<T> deleteByIds(List<T> records) {
        if(records == null || records.isEmpty()) return List.of();
        Class<? extends SQLRecord> recordClass = records.getFirst().getClass();
        List<Long> nonNullPrimaryKeys = records.stream()
                .filter(Objects::nonNull)
                .filter(obj -> obj.getClass() == recordClass)
                .map(SQLRecord::getPrimaryKey).filter(Objects::nonNull).toList();
        if(nonNullPrimaryKeys.isEmpty()){
            logger.error("There seems to be no records that could be deleted here!");
            return List.of();
        }
        return deleteByIds(nonNullPrimaryKeys, (Class<T>)recordClass);
    }

    @Override
    public <T extends SQLRecord> List<T> deleteByIds(List<Long> primaryKeys, Class<T> recordClass) {
        List<Long> nonNullPrimaryKeys = primaryKeys.stream()
                .filter(Objects::nonNull).toList();
        if(nonNullPrimaryKeys.isEmpty()){
            logger.error("There seems to be no records that could be deleted here!");
            return List.of();
        }
        String primaryKeyName = recordClass.getRecordComponents()[0].getName().toLowerCase();
        StringBuilder sb = new StringBuilder("DELETE FROM ").append(recordClass.getSimpleName().toLowerCase());
        sb.append(" WHERE ").append(primaryKeyName).append(" IN (").append(String.join(",", Collections.nCopies(nonNullPrimaryKeys.size(), "?"))).append(");");
        try(PreparedStatement pStmt = borrowConnection().prepareStatement(sb.toString())){
            //Cheeky, but it beats implementing a way to retrieve a list of deleted objects from psql
            // as I'm pretty sure RETURNING * will fail here (though to be fair I only read that, not tested).
            List<T> toBeDeleted = getRecordsByIds(nonNullPrimaryKeys, recordClass);
            for(int i = 0; i < nonNullPrimaryKeys.size(); i++){
                pStmt.setLong(i+1, nonNullPrimaryKeys.get(i));
            }
            pStmt.executeUpdate();
            List<T> areRemaining = getRecordsByIds(nonNullPrimaryKeys, recordClass);
            if (!areRemaining.isEmpty()) {
                logger.error("Some of the primaryKeys failed to delete!");
                toBeDeleted.removeAll(areRemaining);
            }
            return toBeDeleted;
        } catch (SQLException e){
            logger.except("Something went wrong during deletion on table: " + recordClass.getSimpleName(), e);
        }
        return List.of();
    }

    @Override
    public <T extends SQLRecord> List<T> deleteByIds(List<Long> primaryKeys, String tableName) {
        Optional<Class<? extends SQLRecord>> recordClass = dbManager.getRecordClasses().stream()
                .filter(clazz -> clazz.getSimpleName().equalsIgnoreCase(tableName)).findFirst();
        if(recordClass.isEmpty()) {
            logger.error("Could not find a record class that matched that table name: " + tableName);
            return List.of();
        }
        deleteByIds(primaryKeys, recordClass.get());
        return null;
    }

    private <T extends SQLRecord> Map<Class<T>, PreparedStatement> batchRequests(List<T> records, String keyAppend,
                                                                             boolean isUpdate, ConnectionWrapper cw) throws SQLException {

        if(records == null || records.isEmpty()) return Map.of();
        HashMap<Class<T>, PreparedStatement> batchMap = new HashMap<>();
        String className = records.getFirst().getTableName();
        PreparedStatement pStmt = cw.getPreparedStatement(className + keyAppend);
        for(SQLRecord record : records){
            try{
                populateStatement(pStmt, record, isUpdate);
                pStmt.addBatch();
                batchMap.put((Class<T>) record.getClass(), pStmt);
            } catch (Exception e){
                logger.except("Something went wrong during batch preparation for: " + className, e);
            }
        }
        return batchExecute(batchMap);
    }

    @Override
    public <T extends SQLRecord> void batchRequestsNoReturn(List<T> records, String pString, boolean isUpdate) throws SQLException {

        HashMap<Class<T>, PreparedStatement> batchMap = new HashMap<>();
        Connection connection = borrowConnection();
        PreparedStatement pStmt = connection.prepareStatement(pString);
        for(SQLRecord record : records){
            try{
                populateStatement(pStmt, record, isUpdate);
                pStmt.addBatch();
                batchMap.put((Class<T>) record.getClass() , pStmt);
            } catch (Exception e){
                logger.except("Something went wrong during batch preparation! ", e);
            }
        }
        batchExecute(batchMap);
    }

    private <T extends SQLRecord> Map<Class<T>, PreparedStatement> batchExecute
            (Map<Class<T>, PreparedStatement> batchMap){
        batchMap.forEach((className, pStmt) -> {
            try{
                pStmt.executeBatch();
            } catch (SQLException e) {
                logger.except("Exception occurred during batch execution for: " + className, e);
            }
        });
        return batchMap;
    }

    private Map<String, Object> executeSingleQuery(PreparedStatement pStmt) throws SQLException, JsonProcessingException, ClassNotFoundException {
        ResultSet rs = pStmt.executeQuery();
        if(!rs.next()) return null;
        ResultSetMetaData md = rs.getMetaData();
        int columnCount = md.getColumnCount();
        return resultToMap(columnCount, md, rs);
    }

    private Map<String, Object> resultToMap(int columnCount, ResultSetMetaData md, ResultSet rs) throws SQLException, JsonProcessingException, ClassNotFoundException {
        Map<String, Object> recordData = new HashMap<>();
        for(int i = 1; i <= columnCount; i++){
            String columnName = md.getColumnName(i);
            String columnTypeName = md.getColumnTypeName(i);
            Object value;
            if(columnTypeName.equalsIgnoreCase("bool") || columnTypeName.equalsIgnoreCase("boolean")){ //Should be a boolean
                value = rs.getBoolean(i);
            } else if(columnTypeName.equalsIgnoreCase("JSONB")){
                String json = rs.getString(i);
                JsonNode rootNode = om.readTree(json);
                if(rootNode.has("list")){
                    Class<?> clazz = Class.forName(rootNode.get("type").asText());
                    value = om.readValue(json, om.getTypeFactory().constructParametricType(ArrayListHolder.class, clazz));
                } else if(rootNode.has("keyType")){
                    Class<?> keyType = Class.forName(rootNode.get("keyType").asText());
                    Class<?> valueType = Class.forName(rootNode.get("valueType").asText());
                    try{
                        HashMap<String, Object> map = om.readValue(rootNode.get("map").asText(), HashMap.class);
                        value = HashMapHolder.class
                                .getDeclaredConstructor(keyType, valueType, Map.class)
                                .newInstance(keyType, valueType, map);
                    } catch (MismatchedInputException e) {
                        logger.except("MismatchedInputException occurred during HashMapHolder conversion from db!", e);
                        value = new HashMapHolder<>(keyType, valueType);
                    } catch (Exception e) {
                        logger.except("Exception occurred during HashMapHolder conversion from db!", e);
                        value = null;
                    }


                } else {
                    logger.warn("Unknown JSONB type ignored for column named: " + columnName);
                    value = null;
                }
            } else if (md.getColumnType(i) == Types.ARRAY){
                Array array = rs.getArray(i);
                value = List.of((Object[])array.getArray());

            } else {
                value = rs.getObject(i);
            }
            if(value != null) recordData.put(columnName, value);
        }
        return recordData;
    }

    private List<Map<String, Object>> processQueryResultSet(ResultSet rs) throws SQLException {
        List<Map<String, Object>> objMaps = new ArrayList<>();
        if(!rs.next()) return null;
        ResultSetMetaData md = rs.getMetaData();
        int columnCount = md.getColumnCount();
        do{
            try{
                objMaps.add(resultToMap(columnCount, md, rs));
            } catch (Exception e){
                logger.except("Exception occurred while trying to process, query data!");
            }
        } while(rs.next());
        return objMaps;
    }

    private void populateStatement(PreparedStatement pStmt, SQLRecord record, boolean isUpdate)
            throws InvocationTargetException, IllegalAccessException, SQLException, NoSuchFieldException, JsonProcessingException {
        RecordComponent[] comps = record.getClass().getRecordComponents();
        for(int i = 1; i < comps.length; i++){

            Class<?> fieldType = comps[i].getType();
            String fieldName = comps[i].getName();
            Object value = record.getObjectValueByFieldName(fieldName);
            if(value == null) pStmt.setObject(i, null);
            else if(fieldType.equals(LocalDateTime.class)) pStmt.setString(i, value == null ? "" : value.toString());
            else if (fieldType == ArrayListHolder.class || fieldType == HashMapHolder.class){
                pStmt.setString(i, om.writeValueAsString(value));
            } else if(fieldType.isEnum()){
                pStmt.setInt(i, ((Enum<?>)value).ordinal());
            } else pStmt.setObject(i, value);
        }
        if(isUpdate) pStmt.setLong(comps.length, (Long) record.getObjectValueByFieldName(comps[0].getName()));
    }

    /**
     * This is used to reflect an object map where the keys are already .lowerCase() processed
     * to create a record of the input type.
     * @param primaryKey This can be null, purely for logging, otherwise it is not used.
     * @param objMap The object map, based off the return of executeSingleQuery(PreparedStatement)
     * @param recordClass The class to reflect the constructor of... this might be dicey.
     * @return A newly constructed implemented SQLMethod, or null;
     * @throws Exception So many, just assume it failed and you need to read the logs.
     */
    private <T extends SQLRecord> T reflectRecordFromMap(Long primaryKey, Map<String, Object> objMap, Class<T> recordClass) throws Exception {
        if(objMap == null) {
            logger.error(String.format(
                    "No object was returned for primaryKey: %d on table: %s, ignore this if primaryKey may have been invalid.",
                    primaryKey, recordClass.getSimpleName()));
            return null;
        }

        RecordComponent[] comps = recordClass.getRecordComponents();
        Constructor<? extends SQLRecord> constructor = getConstructor(recordClass);
        TreeMap<String, Object> tm = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        tm.putAll(objMap);
        return (T) constructor.newInstance(tm);
    }

    /**
     * {@code WARNING} This can only handle one Record Class at a time! <br><br>
     * This is used to reflect an object map where the keys are already .lowerCase() processed
     * to create a record of the input type.
     * @param objMaps The list of object maps, based off the return of executeSingleQuery(PreparedStatement)
     * @param recordClass The class to reflect the constructor of... this might be dicey.
     * @return A list of newly constructed implemented SQLMethod, or empty list;
     * @throws Exception So many, just assume it failed and you need to read the logs.
     */
    private <T extends SQLRecord> List<T> reflectRecordsFromMaps(List<Map<String, Object>> objMaps, Class<? extends SQLRecord> recordClass) throws Exception {
        if(objMaps == null || objMaps.isEmpty()){
            logger.error("List of object maps was null or empty! Table: " + recordClass.getSimpleName().toLowerCase());
            return List.of();
        }
        Constructor<? extends SQLRecord> constructor = getConstructor(recordClass);
        List<T> records = new ArrayList<>();
        for(Map<String, Object> objMap : objMaps){
            try{
                TreeMap<String, Object> tm = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                tm.putAll(objMap);
                records.add((T) constructor.newInstance(tm));
            } catch (Exception e){
                logger.except("Something went wrong on this reflect record generation!", e);
            }
        }
        return records;
    }

    /**
     * We exclusively use the map constructor now
     * @param recordClass
     * @return
     * @throws NoSuchMethodException
     */
    private Constructor<? extends SQLRecord> getConstructor(Class<? extends SQLRecord> recordClass) throws NoSuchMethodException {
        return recordClass.getDeclaredConstructor(Map.class);
    }
}
