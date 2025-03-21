package com.kovisoft.pg.database.operations;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kovisoft.logger.exports.Logger;
import com.kovisoft.logger.exports.LoggerFactory;
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

            PreparedStatement pStmt = borrowCW().getPreparedStatement(record.getClass().getSimpleName().toLowerCase() + INSERT);
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
                    if(!rs.next()) throw new SQLException("Unable to retrieve inserted ids from execution on " + entry.getKey().getSimpleName());
                    List<Long> generatedIds = new ArrayList<>();
                    do{
                        generatedIds.add(rs.getLong(1));
                    } while(rs.next());
                    return getRecordsByIds(generatedIds, entry.getKey());
                } catch (Exception e){
                    logger.except("Exception occurred during retrieval of generated ids for: " + entry.getKey().getSimpleName(), e);
                }
            }
        } catch (SQLException e) {
            logger.except("Exception occurred during overall add records batching process", e);
        }
        return List.of();
    }


    @Override
    public <T extends SQLRecord> T updateRecord(T record) {
        try{
            PreparedStatement pStmt = borrowCW().getPreparedStatement(record.getClass().getSimpleName().toLowerCase() + UPDATE);
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
                    return getRecordsByIds(records.stream().map(SQLRecord::id).toList(), (Class<T>) records.getFirst().getClass());
                } catch (Exception e){
                    logger.except("Exception occurred during retrieval of generated ids for: " + entry.getKey().getSimpleName(), e);
                }
            }
        } catch (SQLException e) {
            logger.except("Exception occurred during overall add records batching process", e);
        }
        return List.of();
    }

    @Override
    public <T extends SQLRecord> T updateOrAddRecord(T record) {
        return ((record.id() == null) ? addRecord(record) : updateRecord(record));
    }

    @Override
    public <T extends SQLRecord> List<T> updateAndAddRecords(List<T> records) {
        List<T> inserts = records.stream().filter(record -> record.id() == null).toList();
        List<T> updates = records.stream().filter(record -> record.id() != null).toList();
        List<T> returns = new ArrayList<>(records.size());
        returns.addAll(addRecords(inserts));
        returns.addAll(updateRecords(updates));
        return returns;
    }

    @Override
    public <T extends SQLRecord> T getMatch(T record) {
        try {
            PreparedStatement pStmt = borrowCW().getPreparedStatement(record.getClass().getSimpleName().toLowerCase() + MATCH);
            populateStatement(pStmt, record, true); //Bit hacky, but it works so... not a hack ;)
            return record.getNewRecord(executeSingleQuery(pStmt));
        } catch (Exception e) {
            logger.except("Match attempt failed with exception", e);
        }
        return null;
    }

    @Override
    public <T extends SQLRecord> T  getRecordById(Long id, T record) {
        try {
            PreparedStatement pStmt = borrowCW().getPreparedStatement(record.getClass().getSimpleName().toLowerCase() + ID);
            pStmt.setLong(1, id);
            return record.getNewRecord(executeSingleQuery(pStmt));
        } catch (Exception e){
            logger.except("Get record by id failed with exception", e);
        }
        return null;
    }

    @Override
    public <T extends SQLRecord> T  getRecordById(Long id, Class<T> recordClass) {
        try {
            PreparedStatement pStmt = borrowCW().getPreparedStatement(recordClass.getSimpleName().toLowerCase() + ID);
            pStmt.setLong(1, id);
            Map<String, Object> objMap = executeSingleQuery(pStmt);
            return reflectRecordFromMap(id, objMap, recordClass);
        } catch (Exception e){
            logger.except("Get record by id failed with exception", e);
        }
        return null;
    }

    @Override
    public <T extends SQLRecord> T  getRecordById(Long id, String tableName) {
        try {
            PreparedStatement pStmt = borrowCW().getPreparedStatement(tableName.toLowerCase() + ID);
            pStmt.setLong(1, id);
            Map<String, Object> objMap = executeSingleQuery(pStmt);
            Optional<Class<? extends SQLRecord>> recordClass = dbManager.getRecordClasses().stream()
                    .filter(clazz -> clazz.getSimpleName().equalsIgnoreCase(tableName)).findFirst();
            if(recordClass.isEmpty()) throw new IllegalArgumentException("No class matches the table! " + tableName);
            return reflectRecordFromMap(id, objMap, (Class<T>) recordClass.get());
        } catch (Exception e){
            logger.except("Get record by id failed with exception", e);
        }
        return null;
    }

    @Override
    public <T extends SQLRecord> List<T> getRecordsByIds(List<Long> ids, Class<T> recordClass) {
        List<Long> nonNullIds = ids.stream().filter(Objects::nonNull).toList();
        StringBuilder sb = new StringBuilder("SELECT * FROM ").append(recordClass.getSimpleName().toLowerCase());
        sb.append(" WHERE id IN (").append(String.join(",", Collections.nCopies(nonNullIds.size(), "?"))).append(");");
        try(PreparedStatement pStmt = borrowConnection().prepareStatement(sb.toString())){
            for(int i = 0; i < nonNullIds.size(); i++){
                pStmt.setLong(i+1, nonNullIds.get(i));
            }
            ResultSet rs = pStmt.executeQuery();
            List<Map<String, Object>> objMaps = processQueryResultSet(rs);
            if(objMaps == null){
                logger.error("Really not one correct id? You fucking donkey!");
                return List.of();
            }

            return reflectRecordsFromMaps(objMaps, recordClass);
        } catch (Exception e) {
            logger.except("Failed to get records by IDs for Table: " + recordClass.getSimpleName(), e);
        }
        return List.of();
    }

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
            PreparedStatement pStmt = borrowCW().getPreparedStatement(record.getClass().getSimpleName().toLowerCase() + ALL);
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
        if(record.id() == null){
            logger.error("Passed record had a null id, we cannot delete that which does not exist.");
            return null;
        }
        try{
            PreparedStatement pStmt = borrowCW().getPreparedStatement(record.getClass().getSimpleName().toLowerCase() + DELETE);
            pStmt.setLong(1, record.id());
            return record.getNewRecord(executeSingleQuery(pStmt));
        } catch (Exception e) {
            logger.except(String.format("Failed to delete record %d from table: %s", record.id(), record.getClass().getSimpleName()), e);
        }
        return null;
    }

    @Override
    public <T extends SQLRecord> T deleteById(Long id, Class<T> recordClass) {
        if(id == null){
            logger.error("Passed a null id, we cannot delete that which does not exist.");
            return null;
        }
        try{
            PreparedStatement pStmt = borrowCW().getPreparedStatement(recordClass.getSimpleName().toLowerCase() + DELETE);
            pStmt.setLong(1, id);
            return (T) reflectRecordFromMap(id, executeSingleQuery(pStmt), recordClass);
        } catch (Exception e) {
            logger.except(String.format("Failed to delete record %d from table: %s", id, recordClass.getSimpleName()), e);
        }
        return null;
    }

    @Override// Only a single table!
    public <T extends SQLRecord> List<T> deleteByIds(List<T> records) {
        if(records == null || records.isEmpty()) return List.of();
        Class<? extends SQLRecord> recordClass = records.getFirst().getClass();
        List<Long> nonNullIds = records.stream()
                .filter(Objects::nonNull)
                .filter(obj -> obj.getClass() == recordClass)
                .map(SQLRecord::id).filter(Objects::nonNull).toList();
        if(nonNullIds.isEmpty()){
            logger.error("There seems to be no records that could be deleted here!");
            return List.of();
        }
        return deleteByIds(nonNullIds, (Class<T>)recordClass);
    }

    @Override
    public <T extends SQLRecord> List<T> deleteByIds(List<Long> ids, Class<T> recordClass) {
        List<Long> nonNullIds = ids.stream()
                .filter(Objects::nonNull).toList();
        if(nonNullIds.isEmpty()){
            logger.error("There seems to be no records that could be deleted here!");
            return List.of();
        }
        StringBuilder sb = new StringBuilder("DELETE FROM ").append(recordClass.getSimpleName().toLowerCase());
        sb.append(" WHERE id IN (").append(String.join(",", Collections.nCopies(nonNullIds.size(), "?"))).append(");");
        try(PreparedStatement pStmt = borrowConnection().prepareStatement(sb.toString())){
            //Cheeky, but it beats implementing a way to retrieve a list of deleted objects from psql
            // as I'm pretty sure RETURNING * will fail here (though to be fair I only read that, not tested).
            List<T> toBeDeleted = getRecordsByIds(nonNullIds, recordClass);
            for(int i = 0; i < nonNullIds.size(); i++){
                pStmt.setLong(i+1, nonNullIds.get(i));
            }
            pStmt.executeUpdate();
            List<T> areRemaining = getRecordsByIds(nonNullIds, recordClass);
            if (!areRemaining.isEmpty()) {
                logger.error("Some of the ids failed to delete!");
                toBeDeleted.removeAll(areRemaining);
            }
            return toBeDeleted;
        } catch (SQLException e){
            logger.except("Something went wrong during deletion on table: " + recordClass.getSimpleName(), e);
        }
        return List.of();
    }

    @Override
    public <T extends SQLRecord> List<T> deleteByIds(List<Long> ids, String tableName) {
        Optional<Class<? extends SQLRecord>> recordClass = dbManager.getRecordClasses().stream()
                .filter(clazz -> clazz.getSimpleName().equalsIgnoreCase(tableName)).findFirst();
        if(recordClass.isEmpty()) {
            logger.error("Could not find a record class that matched that table name: " + tableName);
            return List.of();
        }
        deleteByIds(ids, recordClass.get());
        return null;
    }

    private <T extends SQLRecord> Map<Class<T>, PreparedStatement> batchRequests(List<T> records, String keyAppend,
                                                                             boolean isUpdate, ConnectionWrapper cw) throws SQLException {

        if(records == null || records.isEmpty()) return Map.of();
        HashMap<Class<T>, PreparedStatement> batchMap = new HashMap<>();
        String className = records.getFirst().getClass().getSimpleName().toLowerCase();
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
                    value = om.readValue(json, om.getTypeFactory().constructParametricType(HashMapHolder.class, keyType, valueType));
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
     * @param id This can be null, purely for logging, otherwise it is not used.
     * @param objMap The object map, based off the return of executeSingleQuery(PreparedStatement)
     * @param recordClass The class to reflect the constructor of... this might be dicey.
     * @return A newly constructed implemented SQLMethod, or null;
     * @throws Exception So many, just assume it failed and you need to read the logs.
     */
    private <T extends SQLRecord> T reflectRecordFromMap(Long id, Map<String, Object> objMap, Class<T> recordClass) throws Exception {
        if(objMap == null) {
            logger.error(String.format(
                    "No object was returned for id: %d on table: %s, ignore this if id may have been invalid.",
                    id, recordClass.getSimpleName()));
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
