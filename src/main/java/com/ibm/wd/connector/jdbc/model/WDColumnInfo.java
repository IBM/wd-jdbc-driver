package com.ibm.wd.connector.jdbc.model;

public class WDColumnInfo {

    private final String columnName;
    private final String columnLabel;
    private final WDFieldType fieldType;
    private final WDFieldPath fieldPath;
    private final String collectionId;
    private final boolean isPrimary;

    public String getColumnName() { return columnName; }
    public String getColumnLabel() { return columnLabel; }
    public WDFieldType getFieldType() { return fieldType; }
    public WDFieldPath getFieldPath() { return fieldPath; }
    public String getCollectionId() { return collectionId; }
    public boolean getIsPrimary() { return isPrimary; }

    private WDColumnInfo(
            String columnName,
            String columnLabel,
            WDFieldType fieldType,
            WDFieldPath fieldPath,
            String collectionId,
            boolean isPrimary
    ) {
        this.columnName = columnName;
        this.columnLabel = columnLabel;
        this.fieldType = fieldType;
        this.fieldPath = fieldPath;
        this.collectionId = collectionId;
        this.isPrimary = isPrimary;
    }

    public WDColumnInfo copy() {
        return new WDColumnInfo(
                columnName,
                columnLabel,
                fieldType,
                fieldPath.copy(),
                collectionId,
                isPrimary
        );
    }

    public static class Builder {
        private String columnName;
        private String columnLabel;
        private WDFieldType fieldType;
        private WDFieldPath fieldPath;
        private String collectionId;
        private boolean isPrimary;

        public Builder columnName(String columnName) {
            this.columnName = columnName;
            return this;
        }

        public Builder columnLabel(String columnLabel) {
            this.columnLabel = columnLabel;
            return this;
        }

        public Builder fieldType(WDFieldType fieldType) {
            this.fieldType = fieldType;
            return this;
        }

        public Builder fieldPath(WDFieldPath fieldPath) {
            this.fieldPath = fieldPath;
            return this;
        }

        public Builder collectionId(String collectionId) {
            this.collectionId = collectionId;
            return this;
        }

        public Builder isPrimary(boolean isPrimary) {
            this.isPrimary = isPrimary;
            return this;
        }

        public Builder copy() {
            Builder newBuilder = new Builder();
            return newBuilder
                    .columnName(this.columnName)
                    .columnLabel(this.columnLabel)
                    .fieldType(this.fieldType)
                    .fieldPath(this.fieldPath)
                    .collectionId(this.collectionId)
                    .isPrimary(this.isPrimary);
        }

        public WDColumnInfo build() {
            return new WDColumnInfo(columnName, columnLabel, fieldType, fieldPath, collectionId, isPrimary);
        }
    }
}
