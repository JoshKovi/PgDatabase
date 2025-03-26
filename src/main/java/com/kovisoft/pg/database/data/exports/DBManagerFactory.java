package com.kovisoft.pg.database.data.exports;

import com.kovisoft.pg.database.manager.DBManagerImpl;
import com.kovisoft.pg.database.operations.DbOperationsAdminUser;
import com.kovisoft.pg.database.operations.DbOperationsBaseUser;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DBManagerFactory {

    private static final HashMap<String, DBManager> MANAGER_MAP = new HashMap<>();
    private static final HashMap<String, DBOperations> OPERATIONS_MAP = new HashMap<>();

    public static DBManager getDBManager(String dbName){;
        return MANAGER_MAP.getOrDefault(dbName, null);
    }

    public static boolean dropDBManager(String dbName) throws Exception{
        DBManager manager = MANAGER_MAP.get(dbName);
        if(manager == null) throw new IllegalArgumentException(dbName + " does not exist!");
        manager.close();
        MANAGER_MAP.remove(dbName);
        return true;
    }

    public static DBOperations getDBOperations(String opName){;
        return OPERATIONS_MAP.getOrDefault(opName, null);
    }

    public static boolean dropDBOperations(String opName) throws Exception{
        DBOperations ops = OPERATIONS_MAP.get(opName);
        if(ops == null) throw new IllegalArgumentException(opName + " does not exist!");
        OPERATIONS_MAP.remove(opName);
        return true;
    }

    public static DBManager createDBManager(DBManagerConfig config) throws SQLException, InterruptedException {
        String dbName = config.getDb();
        if(MANAGER_MAP.containsKey(dbName)) return MANAGER_MAP.get(dbName);
        DBManager manager = new DBManagerImpl(config);
        MANAGER_MAP.put(dbName, manager);
        return manager;
    }

    public static DBManager createDBManager(DBManagerConfig config, AbstractMigration tm) throws SQLException, InterruptedException {
        String dbName = config.getDb();
        if(MANAGER_MAP.containsKey(dbName)) return MANAGER_MAP.get(dbName);
        DBManager manager = new DBManagerImpl(config, List.of(tm));
        MANAGER_MAP.put(dbName, manager);
        return manager;
    }

    public static DBOperations createDBOperations(DBManager dbManager, String opName, boolean adminUser) throws SQLException, InterruptedException {
            if(OPERATIONS_MAP.containsKey(opName)) return OPERATIONS_MAP.get(opName);
            if(adminUser) OPERATIONS_MAP.put(opName, new DbOperationsAdminUser(dbManager));
            else OPERATIONS_MAP.put(opName, new DbOperationsBaseUser(dbManager));
            return OPERATIONS_MAP.get(opName);
    }

    /**
     * Sets up DB Operations for both an admin and user opName = {dbName}-admin or {dbName}-user
     * after initializing the DB manager with its own db name. To retrive use getDBOperations(opName)
     * also the config.getDb() +"-archive" is used as the archive db name. Operations opNames will be
     * either config.getDb() +"-admin" or config.getDb() +"-user" dependent on privileges desired when accessed
     * with getDBOperations(opName). This will only create the DBOperations for the DB in the config.
     * If using multiple databases after migrations you will need to use createDBManager for those
     * separate databases.
     * @param config The DBManagerConfig with the intended configuration for the DBManager
     *               See {@link DBManagerConfig} for more info
     * @param migrations Can be null, only use this in conjunction with appropriate config and a desire to migrate
     *           you db tables. Each List in migrations should be the all the migrations for ONE database,
     *           See {@link AbstractMigration} for more info. All migrations per table List are flattened with
     *           {@link CombinedMigration} which could result in conflicts if not managed carefully.
     */
    public static void overallSetupDB(DBManagerConfig config, Map<String, List<AbstractMigration>> migrations) throws SQLException, InterruptedException {
        List<AbstractMigration> cms = getCombinedMigrations(migrations);

        DBManager manager = new DBManagerImpl(config, cms);
        MANAGER_MAP.put(config.getDb(), manager);
        DBOperations admin = new DbOperationsAdminUser(manager);
        DBOperations user = new DbOperationsBaseUser(manager);
        OPERATIONS_MAP.put(config.getDb() + "-admin", admin);
        OPERATIONS_MAP.put(config.getDb() + "-user", user);
    }

    private static List<AbstractMigration> getCombinedMigrations(Map<String, List<AbstractMigration>> migrations) {
        List<AbstractMigration> cms = new ArrayList<>();
        if(migrations != null && !migrations.isEmpty()){
            for(Map.Entry<String, List<AbstractMigration>> tm : migrations.entrySet()){
                if(tm == null || tm.getValue().isEmpty()) continue;
                AbstractMigration first = tm.getValue().getFirst();
                CombinedMigration cm = new CombinedMigration(first.getArchiveDB(), first.getCurrentDB(), tm.getValue());
                cms.add(cm);
            }
        }
        return cms;
    }


}
