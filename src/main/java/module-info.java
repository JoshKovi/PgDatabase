module PgDatabase {
    requires java.sql;
    requires org.postgresql.jdbc;
    requires Logger;
    requires SimpleConnectionPool;

    exports com.kovisoft.pg.database.data;
    exports com.kovisoft.pg.database.data.exports;

}