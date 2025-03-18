package com.kovisoft.pg.database.operations;

import com.kovisoft.pg.database.data.exports.DBManager;
import com.kovisoft.simple.connection.pool.exports.ConnectionWrapper;

import java.sql.Connection;
import java.sql.SQLException;

public class DbOperationsAdminUser extends DbOperationsBaseUser{

    public DbOperationsAdminUser(){}
    public DbOperationsAdminUser(DBManager dbManager) {
        super(dbManager);
    }

    @Override
    protected Connection borrowConnection() throws SQLException {
        return dbManager.borrowConnection(true);
    }

    @Override
    protected ConnectionWrapper borrowCW() throws SQLException {
        return dbManager.borrowCW(true);
    }
}
