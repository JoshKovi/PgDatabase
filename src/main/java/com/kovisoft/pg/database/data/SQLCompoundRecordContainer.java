package com.kovisoft.pg.database.data;

import java.util.List;

public interface SQLCompoundRecordContainer {

    List<CompoundSQLRecordClass> getCompoundRecords();
    void setCompoundRecords(List<CompoundSQLRecordClass> records);


}
