package com.kovisoft.pg.database.data;

import java.util.List;

public interface CompoundSQLRecord {

    <T extends SQLRecord> T getCompoundRecord() throws IllegalStateException;
    <T extends SQLRecord> T getParentRecord();
    List<? extends SQLRecord> getChildRecords();

    <T extends SQLRecord> void setCompoundRecord(T compoundRecord);
    <T extends SQLRecord> void setParentRecord(T parentRecord);
    void setChildRecords(List<? extends SQLRecord> childRecords);

}
