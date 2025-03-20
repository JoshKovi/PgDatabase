package com.kovisoft.pg.database.data.exports;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kovisoft.logger.exports.Logger;
import com.kovisoft.logger.exports.LoggerFactory;

import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public enum SQLConvertType {
    STRING("TEXT"){},
    LONG("BIGINT"){},
    INT("INT"){},
    INTEGER("INT"){},
    FLOAT("REAL"){},
    DOUBLE("DOUBLE PRECISION"){},
    BOOLEAN("BOOLEAN" ){},
    LOCALDATETIME("TEXT"){},

    STRING_ARRAY("TEXT[]") {},
    LONG_ARRAY("BIGINT[]"){},
    INT_ARRAY("INT[]"){},
    INTEGER_ARRAY("INT[]"){},
    FLOAT_ARRAY("REAL[]"){},
    DOUBLE_ARRAY("DOUBLE PRECISION[]"){},
    BOOLEAN_ARRAY("BOOLEAN[]"){},
    LOCALDATETIME_ARRAY("TEXT[]"){},
    ARRAYLIST("TEXT[]"){},
    LIST("TEXT[]"){},

    MAP("JSONB"){},
    HASHMAP("JSONB"){},
    TREEMAP("JSONB"){},

    ARRAYLISTHOLDER("JSONB"){},
    HASHMAPHOLDER("JSONB"){}


    ;

    private static final Logger logger = LoggerFactory.createStaticLogger(System.getProperty("user.dir") + "/logs",
            "Sql_Type_Converter");
    private static final ObjectMapper om = new ObjectMapper();
    public final String SQL_TYPE;

    SQLConvertType(String sqlType){
        this.SQL_TYPE = sqlType;
    }

    public static SQLConvertType getByClassSimpleName(String simpleName){
        try{
            if(simpleName.indexOf('[') > -1){
                String sqlTypeString = simpleName.substring(0, simpleName.indexOf('[')).toUpperCase() + "_ARRAY";
                return SQLConvertType.valueOf(sqlTypeString);
            }
            return SQLConvertType.valueOf(simpleName.toUpperCase());
        } catch (IllegalArgumentException e){
            LoggerFactory.getLogger("DB_Logger").except("Unsupported db type of " + simpleName, e);
            return null;
        }
    }

    public static SQLConvertType getByClassSimpleName(String simpleName, boolean nonNull){
        SQLConvertType sqlConvertType = getByClassSimpleName(simpleName);
        if(sqlConvertType == null && nonNull)throw new RuntimeException("Tried to get an unsupported type of: " + simpleName);
        return sqlConvertType;
    }

    public boolean isArray(){return this.SQL_TYPE.contains("[]");}
    public boolean isJsonb(){return this.SQL_TYPE.contains("JSONB");}
    public String getArrayType(){
        return this.SQL_TYPE.substring(0, this.SQL_TYPE.length() - 2);
    }

}
