package com.kovisoft.pg.database.data.exports;

import com.fasterxml.jackson.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
public class HashMapHolder<K,V> {

    private final Class<?> keyType;
    private final Class<?> valueType;
    private HashMap<K, V> hashMap;

    public HashMapHolder(Class<K> keyType, Class<V> valueType){
        this.keyType = keyType;
        this.valueType = valueType;
        this.hashMap = new HashMap<>();
    }


    public HashMapHolder(Class<K> keyType, Class<V> valueType,  Map<K, V> map){
        this(keyType, valueType);
        this.hashMap.putAll(map);
    }

    @JsonCreator
    @SuppressWarnings("unchecked")
    public HashMapHolder(@JsonProperty("keyType")Class<K> keyType, @JsonProperty("valueType") Class<V> valueType, @JsonProperty("map") Object map){
        this.keyType = keyType;
        this.valueType = valueType;
        if(map == null){
            this.hashMap = new HashMap<>();
            return;
        }
        if(map.getClass() == HashMapHolder.class){
            this.hashMap = new HashMap<>(((HashMapHolder<K,V>) map).getMap());
            return;
        } else if(!Map.class.isAssignableFrom(map.getClass())){
            throw new IllegalStateException("Object must ba a map!");
        }
        Map<?, ?> objMap = (Map<?, ?>) map;
        this.hashMap = new HashMap<>();
        if(objMap.isEmpty()) return;
        for(Map.Entry<?, ?> entry : objMap.entrySet()){
            try{
                K key = (K) entry.getKey();
                V value = (valueType != ArrayListHolder.class) ? (V) entry.getValue():
                        (V)new ArrayListHolder<>(Class.forName((String) ((Map<String, Object>)entry.getValue()).get("type")),
                                ((Map<String, Object>)entry.getValue()).get("list"));

                this.hashMap.put(key, value);
            } catch (ClassCastException | ClassNotFoundException e){
                throw new IllegalStateException("Map contained one or more elements that could not be converted to the proper key,value pair.");
            }
        }
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

    @JsonIgnore
    public boolean isEmpty(){
        return hashMap.isEmpty();
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

    public void setMap(Map<K,V> map){this.hashMap = new HashMap<K,V>(map);}


}
