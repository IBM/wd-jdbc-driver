package com.ibm.wd.connector.jdbc.sql;

import com.ibm.watson.discovery.v2.Discovery;
import com.ibm.wd.connector.jdbc.WDStatement;
import com.ibm.wd.connector.jdbc.model.WDColumnInfo;
import com.ibm.wd.connector.jdbc.model.WDSchemaInfo;
import com.ibm.wd.connector.jdbc.model.WDTableInfo;
import com.ibm.wd.connector.jdbc.support.WDDocValueExtractor;
import com.ibm.wd.connector.jdbc.support.WDObjectIterator;
import com.ibm.wd.connector.jdbc.support.WDQueryIterator;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.*;
import java.util.Map;
import java.util.Optional;

import static com.ibm.wd.connector.jdbc.WDProperties.WD_GENERATE_SUB_TABLES_STRICTLY;
import static com.ibm.wd.connector.jdbc.model.WDFieldPath.*;

public class WDQueryResultSet extends WDReadOnlyResultSetBase {

    private final WDSchemaInfo schemaInfo;
    private final WDTableInfo tableInfo;
    private final WDObjectIterator objectIterator;
    private Object currentObject;

    public WDQueryResultSet(
            WDStatement statement,
            WDSchemaInfo schemaInfo,
            WDTableInfo tableInfo,
            WDResultSetMetaData.Builder builder,
            Discovery discovery
    ) {
        super(builder, statement);

        this.schemaInfo = schemaInfo;
        this.tableInfo = tableInfo;
        this.objectIterator = new WDObjectIterator(
                new WDQueryIterator(
                        discovery,
                        schemaInfo.getProjectId(),
                        tableInfo.getCollectionId(),
                        statement.getProperties(),
                        statement.getFetchSize()
                ),
                tableInfo.getFieldPathToDoc(),
                Boolean.parseBoolean(WD_GENERATE_SUB_TABLES_STRICTLY.get(statement.getProperties()))
        );
    }

    @Override
    public boolean doNext() throws SQLException {
        if (!objectIterator.hasNext()) {
            return false;
        }
        currentObject = objectIterator.next();
        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object getObject(int columnIndex) throws SQLException {
        if (currentObject == null) {
            throw new SQLException("object not found");
        }

        String columnName = metadata.getColumnName(columnIndex);

        Optional<WDColumnInfo> column = tableInfo.getColumns().stream()
                .filter(info -> info.getColumnName().equals(columnName))
                .findFirst();

        if (!column.isPresent()) {
            throw new SQLException("column " + metadata.getColumnLabel(columnIndex) + " not found");
        }

        final WDColumnInfo columnInfo = column.get();
        if (columnInfo.getFieldPath().equals(RECORD_SEQ_PATH)) {
            return getRow();
        } else if (columnInfo.getFieldPath().equals(FIELD_PATH_PATH)) {
            return ROOT_DOC_PATH.subFieldPath(tableInfo.getFieldPathToDoc());
        }

        if (columnInfo.getFieldPath().isNestedSeqNumField()) {
            return objectIterator
                    .getCurrentSequenceNumberHavingPath(columnInfo.getFieldPath())
                    .getRight();
        }

        Pair<String, Object> objectHoldingValue =
                objectIterator.getCurrentObjectHavingPath(columnInfo.getFieldPath());

        if (!(objectHoldingValue.getRight() instanceof Map)) {
            throw new SQLException("unexpected type");
        }

        return WDDocValueExtractor.extractValue(
                column.get().getFieldPath().getFieldPath().substring(objectHoldingValue.getLeft().length()),
                (Map<String, ?>)objectHoldingValue.getRight()
        );
    }

}
