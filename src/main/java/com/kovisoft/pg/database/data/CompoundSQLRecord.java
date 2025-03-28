package com.kovisoft.pg.database.data;

import com.kovisoft.pg.database.data.exports.ArrayListHolder;

public interface CompoundSQLRecord extends SQLRecord {


    Long getParentKey();
    String getParentTable();
    ArrayListHolder<String> getChildTables();
    ArrayListHolder<Long> getChildKeys();
}
