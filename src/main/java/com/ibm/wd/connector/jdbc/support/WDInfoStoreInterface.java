package com.ibm.wd.connector.jdbc.support;

import com.ibm.wd.connector.jdbc.model.WDSchemaInfo;
import com.ibm.wd.connector.jdbc.model.WDTableInfo;

import java.sql.SQLException;
import java.util.List;

public interface WDInfoStoreInterface {

    List<WDSchemaInfo> fetchSchemaInfoList() throws SQLException;
    List<WDTableInfo> fetchTableInfoList(String projectId) throws SQLException;
    List<WDTableInfo> fetchTableInfoList(String projectId, String collectionId) throws SQLException;
}
