package com.kovisoft.pg.database.operations;

import com.kovisoft.pg.database.data.exports.DBOperations;

public abstract class AbstractDbOperations implements DBOperations {
    //Prepared map statement key postfixes
    public static final String INSERT = "-insert";
    public static final String INSERT_MANY = "-insert-many";
    public static final String UPDATE = "-update";
    public static final String UPDATE_MANY = "-update-many";
    public static final String MATCH = "-match";
    public static final String PRIMARY_KEY = "-pk";
    public static final String ALL = "-all";
    public static final String ALL_LIMIT = "-all-limit";
    public static final String ALL_LIMIT_START = "-all-limit-start";
    public static final String ALL_LIMIT_START_ORDER_DESC = "-all-limit-start-order-desc";
    public static final String ALL_LIMIT_START_ORDER_ASC = "-all-limit-start-order-asc";
    public static final String DELETE = "-delete";
}
