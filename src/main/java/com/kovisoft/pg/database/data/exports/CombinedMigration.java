package com.kovisoft.pg.database.data.exports;

import com.kovisoft.pg.database.data.SQLRecord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CombinedMigration extends AbstractMigration {

    private final String archive;
    private final String current;
    private final Map<String, SQLRecord> migrationMap = new HashMap<>();
    /**
     * This is intended for combining migrations across multiple modules but the same database
     * Ignores input archive and current db names from migrations and combines the migration maps.
     * @param archiveDB The DB to archive to before attempting migration used to terminate connections.
     * @param currentDB The database the data is currently in un-migrated used to terminate connections.
     * @param migrations The list of migrations, all none null/empty migration maps will be combined.
     *                   duplicate keys for migration is not safe and will result in unexpected results.
     */
    public CombinedMigration(String archiveDB, String currentDB, List<AbstractMigration> migrations){
        this.archive = archiveDB;
        this.current = currentDB;
        for(AbstractMigration migration : migrations){
            if(migration == null || migration.getMigrationMap() == null || migration.getMigrationMap().isEmpty()) continue;
            migrationMap.putAll(migration.getMigrationMap());
        }
    }

    @Override
    public Map<String, SQLRecord> getMigrationMap() {
        return migrationMap;
    }

    @Override
    public String getArchiveDB() {
        return archive;
    }

    @Override
    public String getCurrentDB() {
        return current;
    }
}
