package com.kovisoft.pg.database.data;

import com.kovisoft.pg.database.data.exports.ArrayListHolder;
import com.kovisoft.pg.database.data.exports.HashMapHolder;

public interface CompoundSQLRecord extends SQLRecord {


    Long getParentKey();
    String getParentTable();
    HashMapHolder<String, ArrayListHolder<Long>> getChildMap();
//    ArrayListHolder<String> getChildTables();
//    ArrayListHolder<Long> getChildKeys();
}
