package com.kovisoft.pg.database.data.exports;

import com.kovisoft.pg.database.data.SQLRecord;


import java.util.Map;

public abstract class AbstractMigration {

        public abstract Map<String, SQLRecord> getMigrationMap();
        public abstract String getArchiveDB();
        public abstract String getCurrentDB();


}
