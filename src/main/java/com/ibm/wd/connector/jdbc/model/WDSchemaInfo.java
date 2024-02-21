package com.ibm.wd.connector.jdbc.model;

public class WDSchemaInfo {

    private final String schemaName;
    private final String projectName;
    private final String projectId;

    public String getSchemaName() { return schemaName; }
    public String getProjectName() { return projectName; }
    public String getProjectId() { return projectId; }

    public WDSchemaInfo(String projectName, String projectId) {
        this.schemaName = projectName.replace(".", "_");
        this.projectName = projectName;
        this.projectId = projectId;
    }
}
