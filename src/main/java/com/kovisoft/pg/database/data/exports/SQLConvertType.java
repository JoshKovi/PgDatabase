package com.kovisoft.pg.database.data.exports;

import com.kovisoft.logger.exports.LoggerFactory;

public enum SQLConvertType {
    STRING("TEXT"){},
    LONG("BIGINT"){},
    INT("INT"){},
    INTEGER("INT"){},
    FLOAT("REAL"){},
    DOUBLE("DOUBLE PRECISION"){},
    BOOLEAN("BOOLEAN" ){},
    LOCALDATETIME("TEXT"){},

    STRING_ARRAY("TEXT[]"){},
    LONG_ARRAY("BIGINT[]"){},
    INT_ARRAY("INT[]"){},
    INTEGER_ARRAY("INT[]"){},
    FLOAT_ARRAY("REAL[]"){},
    DOUBLE_ARRAY("DOUBLE PRECISION[]"){},
    BOOLEAN_ARRAY("BOOLEAN[]"){},
    LOCALDATETIME_ARRAY("TEXT[]"){},
    ARRAYLIST("TEXT[]"){},
    LIST("TEXT[]"){}


    ;

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

    public String getArrayType(){
        return this.SQL_TYPE.substring(0, this.SQL_TYPE.length() - 2);
    }



}
