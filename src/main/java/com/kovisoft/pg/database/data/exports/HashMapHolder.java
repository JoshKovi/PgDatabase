package com.kovisoft.pg.database.data.exports;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class HashMapHolder<K,V> {

    private final Class<?> keyType;
    private final Class<?> valueType;
    private HashMap<K, V> hashMap;

    public HashMapHolder(Class<?> keyType, Class<?> valueType){
        this.keyType = keyType;
        this.valueType = valueType;
        this.hashMap = new HashMap<>();
    }

    @JsonCreator
    public HashMapHolder(@JsonProperty("keyType") Class<?> keyType, @JsonProperty("valueType") Class<?> valueType, @JsonProperty("map") Map<K, V> map){
        this(keyType, valueType);
        this.hashMap.putAll(map);
    }

    public void put(K key, V value){
        hashMap.put(key, value);
    }

    public V get(K key){
        return hashMap.get(key);
    }

    public boolean containsKey(K key){
        return hashMap.containsKey(key);
    }

    @JsonIgnore
    public int size(){
        return hashMap.size();
    }

    public Map<K, V> getMap(){
        return this.hashMap;
    }

    @JsonIgnore
    public Set<Map.Entry<K, V>> getEntrySet(){
        return this.hashMap.entrySet();
    }

    public Class<?> getKeyType(){
        return keyType;
    }

    public Class<?> getValueType(){
        return valueType;
    }


}
