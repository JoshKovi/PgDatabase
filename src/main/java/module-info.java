module PgDatabase {
    requires java.sql;
    requires org.postgresql.jdbc;
    requires Logger;
    requires SimpleConnectionPool;

    requires com.fasterxml.jackson.annotation;
    requires com.fasterxml.jackson.core;
    requires com.fasterxml.jackson.databind;

    exports com.kovisoft.pg.database.data;
    exports com.kovisoft.pg.database.data.exports;

}