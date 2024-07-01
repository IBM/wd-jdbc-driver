package com.ibm.wd.connector.jdbc.sql;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class WDSQLParserTest {

    private WDSQLParser parser = new WDSQLParser();

    @Test
    public void testSelectAllColumn() {
        SelectStatement statement = parser.parse(
                "select * from \"Sample Project\".\"Sample Collection [wd_root_doc]\"");

        Assertions.assertTrue(statement.isUseAllColumns());
        Assertions.assertEquals("Sample Project", statement.getSchemaName());
        Assertions.assertEquals("Sample Collection [wd_root_doc]", statement.getTableName());
        Assertions.assertNull(statement.getColumnNames());
        Assertions.assertNull(statement.getColumnAliases());
    }

    @Test
    public void testSelectRootDoc() {
        SelectStatement statement = parser.parse("select  "
                + "\"Sample Project\".\"Sample Collection [wd_root_doc]\".parent_document_id ,"
                + " \"Sample Collection [wd_root_doc]\".document_id as doc_id,"
                + "   wd_field_root as  field,"
                + "  \"Sample Project\".\"Sample Collection [wd_root_doc]\".text as t   "
                + "from \"Sample Project\".\"Sample Collection [wd_root_doc]\"");

        Assertions.assertFalse(statement.isUseAllColumns());
        Assertions.assertEquals("Sample Project", statement.getSchemaName());
        Assertions.assertEquals("Sample Collection [wd_root_doc]", statement.getTableName());
        Assertions.assertEquals("parent_document_id", statement.getColumnNames().get(0));
        Assertions.assertEquals(
                "parent_document_id", statement.getColumnAliases().get(0));
        Assertions.assertEquals("document_id", statement.getColumnNames().get(1));
        Assertions.assertEquals("doc_id", statement.getColumnAliases().get(1));
        Assertions.assertEquals("wd_field_root", statement.getColumnNames().get(2));
        Assertions.assertEquals("field", statement.getColumnAliases().get(2));
        Assertions.assertEquals("text", statement.getColumnNames().get(3));
        Assertions.assertEquals("t", statement.getColumnAliases().get(3));
    }

    @Test
    public void testSelectNestedDoc() {
        SelectStatement statement = parser.parse("select  "
                + "\"Sample Project\".\"Sample Collection [metadata]\".parent_document_id , "
                + "\"Sample Collection [metadata]\".document_id as doc_id,   "
                + "wd_field_root as  field,  "
                + "metadata_wd_nested_seq_num  as seq,"
                + "\"Sample Project\".\"Sample Collection [metadata]\".metadata_customer_id as customer_id   "
                + "from \"Sample Project\".\"Sample Collection [metadata]\"");

        Assertions.assertFalse(statement.isUseAllColumns());
        Assertions.assertEquals("Sample Project", statement.getSchemaName());
        Assertions.assertEquals("Sample Collection [metadata]", statement.getTableName());
        Assertions.assertEquals("parent_document_id", statement.getColumnNames().get(0));
        Assertions.assertEquals(
                "parent_document_id", statement.getColumnAliases().get(0));
        Assertions.assertEquals("document_id", statement.getColumnNames().get(1));
        Assertions.assertEquals("doc_id", statement.getColumnAliases().get(1));
        Assertions.assertEquals("wd_field_root", statement.getColumnNames().get(2));
        Assertions.assertEquals("field", statement.getColumnAliases().get(2));
        Assertions.assertEquals(
                "metadata_wd_nested_seq_num", statement.getColumnNames().get(3));
        Assertions.assertEquals("seq", statement.getColumnAliases().get(3));
        Assertions.assertEquals(
                "metadata_customer_id", statement.getColumnNames().get(4));
        Assertions.assertEquals("customer_id", statement.getColumnAliases().get(4));
    }

    @Test
    public void testSelectEnriched() {
        SelectStatement statement = parser.parse(
                "select  "
                        + "\"Sample Project\".\"Sample Collection [enriched_html.contract.elements.location]\".parent_document_id , "
                        + "document_id as doc_id,   "
                        + "wd_field_root as  field,  "
                        + "\"Sample Collection [enriched_html.contract.elements.location]\".enriched_html_tables_body_cells_wd_nested_seq_num  as seq,"
                        + "\"Sample Project\".\"Sample Collection [enriched_html.contract.elements.location]\".enriched_html_tables_body_cells_cell_id as cell_id   "
                        + "from \"Sample Project\".\"Sample Collection [enriched_html.contract.elements.location]\"");

        Assertions.assertFalse(statement.isUseAllColumns());
        Assertions.assertEquals("Sample Project", statement.getSchemaName());
        Assertions.assertEquals(
                "Sample Collection [enriched_html.contract.elements.location]",
                statement.getTableName());
        Assertions.assertEquals("parent_document_id", statement.getColumnNames().get(0));
        Assertions.assertEquals(
                "parent_document_id", statement.getColumnAliases().get(0));
        Assertions.assertEquals("document_id", statement.getColumnNames().get(1));
        Assertions.assertEquals("doc_id", statement.getColumnAliases().get(1));
        Assertions.assertEquals("wd_field_root", statement.getColumnNames().get(2));
        Assertions.assertEquals("field", statement.getColumnAliases().get(2));
        Assertions.assertEquals(
                "enriched_html_tables_body_cells_wd_nested_seq_num",
                statement.getColumnNames().get(3));
        Assertions.assertEquals("seq", statement.getColumnAliases().get(3));
        Assertions.assertEquals(
                "enriched_html_tables_body_cells_cell_id",
                statement.getColumnNames().get(4));
        Assertions.assertEquals("cell_id", statement.getColumnAliases().get(4));
    }
}
