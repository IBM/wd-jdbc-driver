package com.ibm.wd.connector.jdbc.support;

import com.ibm.watson.discovery.v2.model.*;

import java.sql.SQLException;
import java.util.List;

public interface WDClientInterface {

    List<ProjectListDetails> listProjects() throws SQLException;

    List<Field> listFields(String projectId) throws SQLException;

    List<Collection> listCollections(String projectId) throws SQLException;

    CollectionDetails getCollection(String projectId, String collectionId) throws SQLException;
}
