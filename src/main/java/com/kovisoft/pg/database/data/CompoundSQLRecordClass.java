package com.kovisoft.pg.database.data;

import java.util.List;
import java.util.Map;

public abstract class CompoundSQLRecordClass {

    public CompoundSQLRecordClass(){};
    public CompoundSQLRecordClass(Map<String, Object> objMap){};

    public abstract <T extends SQLRecord> T getCompoundRecord() throws IllegalStateException;
    public abstract <T extends SQLRecord> T getParentRecord();
    public abstract List<? extends SQLRecord> getChildRecords();

    public abstract <T extends SQLRecord> void setCompoundRecord(T compoundRecord);
    public abstract <T extends SQLRecord> void setParentRecord(T parentRecord);
    public abstract void setChildRecords(List<? extends SQLRecord> childRecords);
    public abstract <T extends SQLRecord> void addChildRecord(T childRecord);
}
