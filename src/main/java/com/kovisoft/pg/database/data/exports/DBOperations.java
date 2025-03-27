package com.kovisoft.pg.database.data.exports;


import com.kovisoft.pg.database.data.CompoundSQLRecordClass;
import com.kovisoft.pg.database.data.SQLRecord;

import java.lang.reflect.RecordComponent;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface DBOperations {


    //primary DB operations (pass through to SQL methods preferred over getting connection).
    <T extends SQLRecord> T addRecord(T record);
    <T extends SQLRecord> List<T> addRecords(List<T> records);
    <T extends CompoundSQLRecordClass> T addCompoundRecord(T record);
    <T extends CompoundSQLRecordClass> List<T> addCompoundRecords(List<T> records);

    <T extends SQLRecord> T updateRecord(T record);
    <T extends SQLRecord> List<T> updateRecords(List<T> records);
    <T extends CompoundSQLRecordClass> T updateCompoundRecord(T record);
    <T extends CompoundSQLRecordClass> List<T> updateCompoundRecords(List<T> records);

    <T extends SQLRecord> T updateOrAddRecord(T record);
    <T extends SQLRecord> List<T> updateAndAddRecords(List<T> records);
    <T extends CompoundSQLRecordClass> T updateOrAddCompoundRecord(T record);
    <T extends CompoundSQLRecordClass> List<T> updateOrAddCompoundRecords(List<T> records);

    <T extends SQLRecord> T getMatch(T record);
    <T extends CompoundSQLRecordClass> T getCompoundMatch(T record);

    <T extends SQLRecord> T getRecordById(Long id, T record);
    <T extends SQLRecord> T getRecordById(Long id, Class<T> recordClass);
    <T extends SQLRecord> T getRecordById(Long id, String tableName);
    <T extends SQLRecord> List<T> getRecordsByIds(List<Long> ids, Class<T> recordClass);

    <T extends CompoundSQLRecordClass> T getCompoundRecordById(Long id, T record);
    <T extends CompoundSQLRecordClass> T getCompoundRecordById(Long id, Class<T> recordClass);
    <T extends CompoundSQLRecordClass> T getCompoundRecordById(Long id, String tableName);
    <T extends CompoundSQLRecordClass> List<T> getCompoundRecordsById(List<Long> ids, Class<T> recordClass);

    <T extends SQLRecord> List<T> getMatchByColumnNames(T record, List<String> columnNames);
    <T extends SQLRecord> List<T> getMatchByComponents(T record, RecordComponent[] components);

    <T extends SQLRecord> List<T> getAllEntries(T record);
    <T extends SQLRecord> List<T> getAllEntries(Class<T> recordClass);
    <T extends SQLRecord> List<T> getAllEntries(Class<T> recordClass, int limit);
    <T extends SQLRecord> List<T> getAllEntries(Class<T> recordClass, int limit, int startIndex);
    <T extends SQLRecord> List<T> getAllEntriesAscending(Class<T> recordClass, int limit, int startIndex, String columnName);
    <T extends SQLRecord> List<T> getAllEntriesDescending(Class<T> recordClass, int limit, int startIndex, String columnName);


    List<Map<String, Object>> getAllEntriesAsMaps(String tableName);

    <T extends SQLRecord> T deleteById(T record);
    <T extends SQLRecord> T deleteById(Long id, Class<T> recordClass);
    <T extends SQLRecord> List<T> deleteByIds(List<T> records);
    <T extends SQLRecord> List<T> deleteByIds(List<Long> ids, Class<T> recordClass);
    <T extends SQLRecord> List<T> deleteByIds(List<Long> ids, String tableName);

    <T extends SQLRecord> void batchRequestsNoReturn(List<T> records, String pString, boolean isUpdate) throws SQLException;

}
