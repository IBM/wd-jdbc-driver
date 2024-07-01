package com.ibm.wd.connector.jdbc;

import static com.ibm.wd.connector.jdbc.model.WDConstants.USERNAME_BEARER;

import java.util.Properties;

public enum WDProperties {
    WD_USER("user", USERNAME_BEARER),
    WD_PASSWORD("password", "bearer_token"),
    WD_IAM_API("iamapi", "https://iam.cloud.ibm.com"),
    WD_DEFAULT_FETCH_SIZE("wdDefaultFetchSize", "10"),
    WD_CURSOR_KEY_FIELD_PATH("wdCursorKeyFieldPath", "metadata.wd_cursor_key"),
    WD_GENERATE_SUB_TABLES_STRICTLY("wdGenerateSubTablesStrictly", "true"),
    WD_COLUMN_SEPARATOR("wdColumnSeparator", "_"),
    WD_PROJECT_IDS_TO_LIST_SCHEMA("wdProjectIdsToListSchema", "");

    private final String name;
    private final String defaultValue;

    WDProperties(String name, String defaultValue) {
        this.name = name;
        this.defaultValue = defaultValue;
    }

    public String getName() {
        return name;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public String get(Properties properties) {
        return properties.getProperty(name, defaultValue);
    }
}
