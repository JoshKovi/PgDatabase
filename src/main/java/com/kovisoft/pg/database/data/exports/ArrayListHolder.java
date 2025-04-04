package com.kovisoft.pg.database.data.exports;

import com.fasterxml.jackson.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Collection;

/**
 * Barebones JSONB Arraylist wrapper, besides get and put
 * everything should be done after using getList. This
 * instantiates a new list so the original list is not
 * actually connected to the list inside.
 * @param <T> The Type parameter aka the class.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
public class ArrayListHolder <T>{

    private ArrayList<T> list;
    private final Class<?> clazz;

    /**
     * Instantiates a new ArrayList of Type T
     * @param type The class of data held to be held in this list.
     */
    public ArrayListHolder(Class<T> type){
        clazz = type;
        list = new ArrayList<>();
    }

    /**
     * Instantiates a new ArrayList of Type T
     * with the data held in list.
     * @param type The Class of the data held in List.
     * @param list The list containing the data of type.
     */
    public ArrayListHolder( Class<T> type, List<T> list){
        clazz = type;
        this.list = new ArrayList<>(list);
    }

    /**
     * Attempts to instantiate the list given its type and the
     * Object containing a list of that type
     * @param type The type of the list.
     * @param list The Object containing an AssignableFrom List object.
     * @throws IllegalStateException If the Object fails the List.class.isAssignableFrom check or
     * contains an element that can not be cast to T.
     */
    @JsonCreator
    @SuppressWarnings("unchecked")
    public ArrayListHolder(@JsonProperty("type")Class<T> type, @JsonProperty("list")Object list){
        clazz = type;
        if(list == null){
            this.list = new ArrayList<>();
            return;
        }
        if(list.getClass() == ArrayListHolder.class) {
            this.list = ((ArrayListHolder<T>) list).getList();
            return;
        } else if(!(List.class.isAssignableFrom(list.getClass()))) {
            throw new IllegalStateException("Object must be a List!");
        }
        List<?> objList = (List<?>) list;
        this.list = new ArrayList<>();
        if(objList.isEmpty()){ return;}
        for(Object obj : objList){
            try{
                if(clazz == Long.class && obj.getClass() == Integer.class){
                    obj = Long.parseLong(obj.toString());
                }
                T item = (T) obj;
                this.list.add(item);
            } catch (ClassCastException e){
                throw new IllegalStateException("List contained an element that could not be converted to appropriate type.");
            }
        }
    }

    /**
     * Wrapper for {@link ArrayList#add(Object)}
     * @param item Item to be added to the list
     * @return True as Specified by {@link Collection#add}
     */
    public boolean add(T item){
        return list.add(item);
    }

    /**
     * Wrapper for {@link ArrayList#addAll(Collection)}
     * @param items Items to be added to the list
     * @return True as Specified by {@link ArrayList#addAll(Collection)}
     * @throws NullPointerException – if the specified collection is null
     */
    public boolean addAll(Collection<T> items){
        return list.addAll(items);
    }

    /**
     * Wrapper for {@link ArrayList#get(int)}
     * @param index The index of the item to be retrieved
     * @return The item retried at index.
     * @throws IndexOutOfBoundsException – if the index is out of range (index < 0 || index >= size())
     */
    public T get(int index){
        return list.get(index);
    }

    /**
     * Returns the size of the underlying {@link ArrayList}.
     * @return The number of elements in this List.
     */
    @JsonIgnore
    public int size(){
        return list.size();
    }

    /**
     * Get the underlying list
     * @return The underlying mutable {@link ArrayList}.
     */
    public ArrayList<T> getList(){
        return list;
    }

    /**
     * Gets the associated class of this list.
     * @return T Class
     */
    public Class<?> getType(){
        return clazz;
    }

    @JsonIgnore
    public boolean isEmpty(){
        return list.isEmpty();
    }

    @JsonIgnore
    public T getFirst(){
        return list.getFirst();
    }

    /**
     * Gets the class.getSimpleName of this list.
     * @return The simple name of the Class.
     */
    @JsonIgnore
    public String getSimpleName(){
        return clazz.getSimpleName();
    }

    /**
     * Sets the underlying list to a new Arraylist of
     * the elements in list.
     * @param list The list containing the data of this wrappers type.
     */
    public void setList(List<T> list){
        this.list = new ArrayList<>(list);
    }

}
