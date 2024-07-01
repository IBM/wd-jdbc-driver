package com.ibm.wd.connector.jdbc.model;

import com.ibm.watson.discovery.v2.model.Field;

import java.sql.JDBCType;
import java.time.LocalDateTime;

public enum WDFieldType {

    STRING(Field.Type.STRING, JDBCType.VARCHAR, String.class, 0, "'", "'", true, false, true, false,true),
    LONG(Field.Type.X_LONG, JDBCType.REAL, Long.class, 19, null, null, false, false, true, false,true),
    DOUBLE(Field.Type.X_DOUBLE, JDBCType.DOUBLE, Double.class, 76, null, null, false, false, true, false,true),
    BOOLEAN(Field.Type.X_BOOLEAN, JDBCType.BOOLEAN, Boolean.class, 1, null, null, false, false, true, false, true),
    DATE(Field.Type.DATE, JDBCType.DATE, LocalDateTime.class, 19, "'", "'", true, false, true, false,true),
    NESTED(Field.Type.NESTED, JDBCType.CLOB, String.class, 0, "'", "'", true, false, true, false, false),
    HTML("html", JDBCType.CLOB, String.class, 0, "'", "'", true, false, true, false, false);

    public static final String HTML_FIELD_TYPE = "html";

    final String wdType;
    final JDBCType jdbcType;
    final Class<?> clazz;
    final int precision;
    final String literalPrefix;
    final String literalSuffix;
    final boolean caseSensitive;
    final boolean unsigned;
    final boolean concrete;
    final boolean udt;
    final boolean canSupportMultiValue;

    WDFieldType(String wdType, JDBCType jdbcType, Class<?> clazz,
                int precision, String literalPrefix, String literalSuffix,
                boolean caseSensitive, boolean unsigned, boolean concrete, boolean udt,
                boolean canSupportMultiValue) {
        this.wdType = wdType;
        this.jdbcType = jdbcType;
        this.clazz = clazz;
        this.precision = precision;
        this.literalPrefix = literalPrefix;
        this.literalSuffix = literalSuffix;
        this.caseSensitive = caseSensitive;
        this.unsigned = unsigned;
        this.concrete = concrete;
        this.udt = udt;
        this.canSupportMultiValue = canSupportMultiValue;
    }

    public String getWdType() { return wdType; }
    public JDBCType getJdbcType() { return jdbcType; }
    public Class<?> getClazz() { return clazz; }
    public int getPrecision() { return precision; }
    public String getLiteralPrefix() { return literalPrefix; }
    public String getLiteralSuffix() { return literalSuffix; }
    public boolean isCaseSensitive() { return caseSensitive; }
    public boolean isUnsigned() { return unsigned; }
    public boolean isConcrete() { return concrete; }
    public boolean isUdt() { return udt; }
    public boolean isCanSupportMultiValue() { return canSupportMultiValue; }

    public static WDFieldType resolveByWDType(String wdType) {
        for (WDFieldType fieldType : WDFieldType.values()) {
            if (fieldType.wdType.equals(wdType)) {
                return fieldType;
            }
        }
        return null;
    }
}
