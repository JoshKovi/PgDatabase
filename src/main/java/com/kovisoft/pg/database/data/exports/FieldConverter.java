package com.kovisoft.pg.database.data.exports;

import java.util.function.Function;

public class FieldConverter {
    public final String originalColumnName;
    public final String newColumnName;
    public final Function<Object, Object> typeConverterFunction;

    public FieldConverter(String originalColumnName, String newColumnName) {
        this.originalColumnName = originalColumnName;
        this.newColumnName = newColumnName;
        this.typeConverterFunction = null;
    }

    public FieldConverter(String originalColumnName, String newColumnName, Function<Object, Object> typeConverterFunction) {
        this.originalColumnName = originalColumnName;
        this.newColumnName = newColumnName;
        this.typeConverterFunction = typeConverterFunction;

    }

    public Object typeConverter(Object originalValue) throws Exception {
        return originalValue;
    }
}
