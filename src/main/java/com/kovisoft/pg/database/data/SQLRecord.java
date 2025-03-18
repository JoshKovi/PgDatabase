package com.kovisoft.pg.database.data;


import com.kovisoft.pg.database.data.exports.FieldConverter;

import java.util.*;

public interface SQLRecord {
    // Each interface needs a constructor of type Map<String, Object> to work with most database operations
    // For ease I recommend using a TreeMap with String.CASE_INSENSITIVE_ORDER set.

    Long id();

    /**
     * This is used to call a Map constructor that must be defined to handle
     * edge cases in the database.
     * @param objectMap Should have all the values in their expected format with potentially
     *                  case sensitive keys (Use a tree map)
     * @return A new instance of the object.
     * @param <T>
     */
    <T extends SQLRecord> T getNewRecord(Map<String, Object> objectMap);
    <T extends SQLRecord> boolean equalsWithoutId(T record);

    // Used for migrations of the database
    default List<FieldConverter> getConverter(){
        return List.of();
    }

    //Gets around pesky reflection issues with decentralize database types.
    Object getObjectValueByFieldName(String fieldName) throws NoSuchFieldException, IllegalAccessException;



}
