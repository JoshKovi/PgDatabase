package com.kovisoft.pg.database.data;


import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class Records {

    public static LocalDateTime getLocalDtOrNull(Object dateTime){
        if(dateTime == null) return null;
        if(dateTime.getClass() ==  LocalDateTime.class) return (LocalDateTime) dateTime;
        try {
            return LocalDateTime.parse((String) dateTime);
        } catch (Exception e){
            return null;
        }
    }

    public static Long getLongOrNull(Object longObj){
        if(longObj == null) return null;
        if(longObj instanceof Long) return (Long) longObj;
        if(longObj instanceof Integer) return Long.parseLong(longObj.toString());
        return Long.parseLong((String)longObj);
    }

    public static Float getFloatOrNull(Object floatObj){
        if(floatObj == null) return null;
        if(floatObj instanceof Float) return (Float) floatObj;
        if(floatObj instanceof Integer) return Float.parseFloat(floatObj.toString());
        if(floatObj instanceof Double) return Float.parseFloat(floatObj.toString());
        return Float.parseFloat((String)floatObj);
    }

    public static Integer getIntegerOrNull(Object intObj){
        if(intObj == null) return null;
        if(intObj instanceof Integer) return (Integer) intObj;
        return Integer.parseInt((String)intObj);
    }

    public static ArrayList<?> getArrayList(Object obj){
        if(obj == null) return new ArrayList<>();
        Class<?> clazz = obj.getClass();
        if(clazz.isArray()) return new ArrayList<>(List.of((Object[]) obj));
        if(obj instanceof List<?>) return new ArrayList<>((List<Object>)obj);
        if(clazz == ArrayList.class) return (ArrayList<Object>)obj;
        return new ArrayList<>();
    }

}
