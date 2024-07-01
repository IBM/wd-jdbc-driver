package com.ibm.wd.connector.jdbc.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class WDTableInfo {

    private static final String TABLE_NAME_FORMAT = "%s [%s]";

    private final String tableName;
    private final String collectionName;
    private final WDFieldPath fieldPathToDoc;
    private final String collectionId;
    private final String projectId;
    private final String description;
    private final List<WDColumnInfo> columns;

    public String getTableName() { return tableName; }
    public String getCollectionName() { return collectionName; }
    public WDFieldPath getFieldPathToDoc() { return fieldPathToDoc; }
    public String getCollectionId() { return collectionId; }
    public String getProjectId() { return projectId; }
    public String getDescription() { return description; }
    public List<WDColumnInfo> getColumns() { return columns; }

    public Optional<WDColumnInfo> findColumnByName(String name) {
        return columns.stream().filter(column -> column.getColumnName().equals(name)).findFirst();
    }

    private WDTableInfo(
            String collectionName,
            WDFieldPath fieldPathToDoc,
            String collectionId,
            String projectId,
            String description,
            List<WDColumnInfo> columns
    ) {
        this.collectionName = collectionName;
        this.fieldPathToDoc = fieldPathToDoc;
        this.collectionId = collectionId;
        this.projectId = projectId;
        this.description = description;
        this.columns = columns;
        this.tableName = String.format(
                TABLE_NAME_FORMAT,
                this.collectionName.replace(".", "_"), this.fieldPathToDoc.getLabel()
        );
    }

    public static class Builder {

        private String collectionName;
        private String collectionId;
        private String projectId;
        private WDFieldPath fieldPathToDoc;
        private String description;
        private final List<WDColumnInfo> columns = new ArrayList<>();

        public Builder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public Builder collectionId(String collectionId) {
            this.collectionId = collectionId;
            return this;
        }

        public Builder projectId(String projectId) {
            this.projectId = projectId;
            return this;
        }

        public Builder fieldPathToDoc(WDFieldPath fieldPathToDoc) {
            this.fieldPathToDoc = fieldPathToDoc;
            return this;
        }

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public Builder addColumn(WDColumnInfo field) {
            columns.add(field);
            return this;
        }

        public Builder removeColumnInfo(WDColumnInfo field) {
            columns.remove(field);
            return this;
        }

        public Builder removeColumnInfo(WDFieldPath fieldPath) {
            columns.removeIf((WDColumnInfo f) -> f.getFieldPath().getFieldPath().equals(fieldPath.getFieldPath()));
            return this;
        }

        public Builder removePrefixMatchedMultiColumnInfo(WDFieldPath fieldPath) {
            columns.removeIf((WDColumnInfo f) -> f.getFieldPath().getFieldPath().startsWith(fieldPath.getFieldPath()));
            return this;
        }

        public Builder clearColumnInfo() {
            columns.clear();
            return this;
        }

        public Builder replaceColumnInfoWith(List<WDColumnInfo> columns) {
            this.columns.clear();
            this.columns.addAll(columns);
            return this;
        }

        public WDTableInfo.Builder copy() {
            WDTableInfo.Builder builder = new WDTableInfo.Builder();
            builder.collectionName = this.collectionName;
            builder.collectionId = this.collectionId;
            builder.description = this.description;
            builder.projectId = this.projectId;
            builder.fieldPathToDoc = this.fieldPathToDoc;
            for (WDColumnInfo columnInfo : this.columns) {
                builder.addColumn(columnInfo.copy());
            }
            return builder;
        }

        public WDTableInfo build() {
            return
                    new WDTableInfo(
                            collectionName,
                            fieldPathToDoc,
                            collectionId,
                            projectId,
                            description,
                            new ArrayList<>(columns)
                    );
        }
    }
}
