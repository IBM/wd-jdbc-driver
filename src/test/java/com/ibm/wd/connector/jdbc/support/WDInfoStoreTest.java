package com.ibm.wd.connector.jdbc.support;

import static com.ibm.wd.connector.jdbc.WDProperties.WD_PROJECT_IDS_TO_LIST_SCHEMA;

import com.ibm.watson.discovery.v2.model.GetCollectionOptions;
import com.ibm.watson.discovery.v2.model.ListCollectionsOptions;
import com.ibm.watson.discovery.v2.model.ListFieldsOptions;
import com.ibm.wd.connector.jdbc.DiscoveryV2Factory;
import com.ibm.wd.connector.jdbc.model.WDColumnInfo;
import com.ibm.wd.connector.jdbc.model.WDFieldType;
import com.ibm.wd.connector.jdbc.model.WDSchemaInfo;
import com.ibm.wd.connector.jdbc.model.WDTableInfo;
import com.ibm.wd.connector.jdbc.utils.DiscoveryV2ForTestFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

public class WDInfoStoreTest {

    private DiscoveryV2Factory factory;

    private WDInfoStore getInfoStore(Properties properties) throws IOException, SQLException {
        if (factory == null) {
            factory = new DiscoveryV2ForTestFactory.Builder()
                    .importListProjects("projects.json")
                    .importListCollections(
                            new ListCollectionsOptions.Builder()
                                    .projectId("e6402476-1bbb-4971-9839-c062e593a7b3")
                                    .build(),
                            "collections.json")
                    .importListFields(
                            new ListFieldsOptions.Builder()
                                    .projectId("e6402476-1bbb-4971-9839-c062e593a7b3")
                                    .build(),
                            "fields.json")
                    .importCollectionDetails(
                            new GetCollectionOptions.Builder()
                                    .projectId("e6402476-1bbb-4971-9839-c062e593a7b3")
                                    .collectionId("8357c1c6-1742-24c1-0000-018d1123e258")
                                    .build(),
                            "collection1.json")
                    .importCollectionDetails(
                            new GetCollectionOptions.Builder()
                                    .projectId("e6402476-1bbb-4971-9839-c062e593a7b3")
                                    .collectionId("7357c1c6-1742-24c1-0000-018d1123e258")
                                    .build(),
                            "collection2.json")
                    .build();
        }
        return new WDInfoStore(factory.create(properties), properties);
    }

    @Test
    public void testFetchSchemaInfo() throws SQLException, IOException {
        Properties properties = new Properties();
        WDInfoStore infoStore = getInfoStore(properties);
        List<WDSchemaInfo> schemaInfoList = infoStore.fetchSchemaInfoList();

        Assertions.assertEquals(5, schemaInfoList.size());
        Optional<WDSchemaInfo> schema = schemaInfoList.stream()
                .filter(schemaInfo -> Objects.equals(
                        schemaInfo.getProjectId(), "4ed0983d-740b-4002-963f-f4aaa7ff0905"))
                .findFirst();
        Assertions.assertTrue(schema.isPresent());
        Assertions.assertEquals("Sample Project", schema.get().getSchemaName());
        Assertions.assertEquals("Sample Project", schema.get().getProjectName());

        schema = schemaInfoList.stream()
                .filter(schemaInfo -> Objects.equals(
                        schemaInfo.getProjectId(), "16402476-1bbb-4971-9839-c062e593a7b2"))
                .findFirst();
        Assertions.assertTrue(schema.isPresent());
        Assertions.assertEquals("Test_DR_Project", schema.get().getSchemaName());
        Assertions.assertEquals("Test.DR.Project", schema.get().getProjectName());
    }

    @Test
    public void testFetchSchemaInfoWithProjectFiltered() throws SQLException, IOException {

        Properties properties = new Properties();
        properties.setProperty(
                WD_PROJECT_IDS_TO_LIST_SCHEMA.getName(),
                "4ed0983d-740b-4002-963f-f4aaa7ff0905,16402476-1bbb-4971-9839-c062e593a7b2");
        WDInfoStore infoStore = getInfoStore(properties);
        List<WDSchemaInfo> schemaInfoList = infoStore.fetchSchemaInfoList();

        Assertions.assertEquals(2, schemaInfoList.size());
        Assertions.assertTrue(schemaInfoList.stream()
                .anyMatch(schemaInfo -> Objects.equals(
                        schemaInfo.getProjectId(), "4ed0983d-740b-4002-963f-f4aaa7ff0905")));
        Assertions.assertTrue(schemaInfoList.stream()
                .anyMatch(schemaInfo -> Objects.equals(
                        schemaInfo.getProjectId(), "16402476-1bbb-4971-9839-c062e593a7b2")));
    }

    @Test
    public void testFetchTableInfoOfEnrichedField() throws SQLException, IOException {

        Properties properties = new Properties();
        WDInfoStore infoStore = getInfoStore(properties);
        List<WDTableInfo> schemaInfoList = infoStore.fetchTableInfoList(
                "e6402476-1bbb-4971-9839-c062e593a7b3", "8357c1c6-1742-24c1-0000-018d1123e258");

        Assertions.assertEquals(22, schemaInfoList.size());

        Optional<WDTableInfo> table = schemaInfoList.stream()
                .filter(schemaInfo -> Objects.equals(
                        schemaInfo.getTableName(),
                        "Annual Report Collection 1 [entities from enriched_text]"))
                .findFirst();
        Assertions.assertTrue(table.isPresent());
        Assertions.assertEquals("Annual Report Collection 1", table.get().getCollectionName());
        Assertions.assertEquals(
                "8357c1c6-1742-24c1-0000-018d1123e258", table.get().getCollectionId());
        Assertions.assertEquals(
                "_.enriched_text.entities.mentions",
                table.get().getFieldPathToDoc().getFieldPath());
        Assertions.assertEquals(
                "ENTITIES",
                table.get().getFieldPathToDoc().getOptionalEnrichFieldInfo().name());

        List<WDColumnInfo> columns = table.get().getColumns();
        Assertions.assertEquals(13, columns.size());
        Optional<WDColumnInfo> column = columns.stream()
                .filter(columnInfo ->
                        Objects.equals(columnInfo.getColumnName(), "parent_document_id"))
                .findFirst();
        Assertions.assertTrue(column.isPresent());
        Assertions.assertEquals("parent_document_id", column.get().getColumnLabel());
        Assertions.assertEquals(WDFieldType.STRING, column.get().getFieldType());
        Assertions.assertEquals(
                "_.metadata.parent_document_id", column.get().getFieldPath().getFieldPath());

        column = columns.stream()
                .filter(columnInfo -> Objects.equals(columnInfo.getColumnName(), "document_id"))
                .findFirst();
        Assertions.assertTrue(column.isPresent());
        Assertions.assertEquals("document_id", column.get().getColumnLabel());
        Assertions.assertEquals(WDFieldType.STRING, column.get().getFieldType());
        Assertions.assertEquals("_.document_id", column.get().getFieldPath().getFieldPath());

        column = columns.stream()
                .filter(columnInfo -> Objects.equals(columnInfo.getColumnName(), "wd_field_path"))
                .findFirst();
        Assertions.assertTrue(column.isPresent());
        Assertions.assertEquals("wd_field_path", column.get().getColumnLabel());
        Assertions.assertEquals(WDFieldType.STRING, column.get().getFieldType());
        Assertions.assertEquals("_.", column.get().getFieldPath().getFieldPath());

        column = columns.stream()
                .filter(columnInfo -> Objects.equals(
                        columnInfo.getColumnName(), "enriched_text_wd_nested_seq_num"))
                .findFirst();
        Assertions.assertTrue(column.isPresent());
        Assertions.assertEquals("enriched_text_wd_nested_seq_num", column.get().getColumnLabel());
        Assertions.assertEquals(WDFieldType.LONG, column.get().getFieldType());
        Assertions.assertEquals(
                "_.enriched_text.wd_nested_seq_num", column.get().getFieldPath().getFieldPath());

        column = columns.stream()
                .filter(columnInfo -> Objects.equals(
                        columnInfo.getColumnName(), "enriched_text_entities_wd_nested_seq_num"))
                .findFirst();
        Assertions.assertTrue(column.isPresent());
        Assertions.assertEquals(
                "enriched_text_entities_wd_nested_seq_num", column.get().getColumnLabel());
        Assertions.assertEquals(WDFieldType.LONG, column.get().getFieldType());
        Assertions.assertEquals(
                "_.enriched_text.entities.wd_nested_seq_num",
                column.get().getFieldPath().getFieldPath());

        column = columns.stream()
                .filter(columnInfo -> Objects.equals(
                        columnInfo.getColumnName(),
                        "enriched_text_entities_mentions_wd_nested_seq_num"))
                .findFirst();
        Assertions.assertTrue(column.isPresent());
        Assertions.assertEquals(
                "enriched_text_entities_mentions_wd_nested_seq_num",
                column.get().getColumnLabel());
        Assertions.assertEquals(WDFieldType.LONG, column.get().getFieldType());
        Assertions.assertEquals(
                "_.enriched_text.entities.mentions.wd_nested_seq_num",
                column.get().getFieldPath().getFieldPath());

        column = columns.stream()
                .filter(columnInfo -> Objects.equals(
                        columnInfo.getColumnName(), "enriched_text_entities_mentions_confidence"))
                .findFirst();
        Assertions.assertTrue(column.isPresent());
        Assertions.assertEquals(
                "enriched_text_entities_mentions_confidence", column.get().getColumnLabel());
        Assertions.assertEquals(WDFieldType.DOUBLE, column.get().getFieldType());
        Assertions.assertEquals(
                "_.enriched_text.entities.mentions.confidence",
                column.get().getFieldPath().getFieldPath());

        column = columns.stream()
                .filter(columnInfo -> Objects.equals(
                        columnInfo.getColumnName(),
                        "enriched_text_entities_mentions_location_begin"))
                .findFirst();
        Assertions.assertTrue(column.isPresent());
        Assertions.assertEquals(
                "enriched_text_entities_mentions_location_begin", column.get().getColumnLabel());
        Assertions.assertEquals(WDFieldType.LONG, column.get().getFieldType());
        Assertions.assertEquals(
                "_.enriched_text.entities.mentions.location.begin",
                column.get().getFieldPath().getFieldPath());
    }

    @Test
    public void testFetchTableInfoOfEnrichedHtmlField() throws SQLException, IOException {

        Properties properties = new Properties();
        WDInfoStore infoStore = getInfoStore(properties);
        List<WDTableInfo> schemaInfoList = infoStore.fetchTableInfoList(
                "e6402476-1bbb-4971-9839-c062e593a7b3", "8357c1c6-1742-24c1-0000-018d1123e258");

        Assertions.assertEquals(22, schemaInfoList.size());

        Optional<WDTableInfo> table = schemaInfoList.stream()
                .filter(schemaInfo -> Objects.equals(
                        schemaInfo.getTableName(),
                        "Annual Report Collection 1 [tables from enriched_html]"))
                .findFirst();
        Assertions.assertTrue(table.isPresent());
        Assertions.assertEquals("Annual Report Collection 1", table.get().getCollectionName());
        Assertions.assertEquals(
                "8357c1c6-1742-24c1-0000-018d1123e258", table.get().getCollectionId());
        Assertions.assertEquals(
                "_.enriched_html.tables.body_cells",
                table.get().getFieldPathToDoc().getFieldPath());
        Assertions.assertEquals(
                "TABLES",
                table.get().getFieldPathToDoc().getOptionalEnrichFieldInfo().name());

        List<WDColumnInfo> columns = table.get().getColumns();
        Assertions.assertEquals(20, columns.size());
        Optional<WDColumnInfo> column = columns.stream()
                .filter(columnInfo ->
                        Objects.equals(columnInfo.getColumnName(), "parent_document_id"))
                .findFirst();
        Assertions.assertTrue(column.isPresent());
        Assertions.assertEquals("parent_document_id", column.get().getColumnLabel());
        Assertions.assertEquals(WDFieldType.STRING, column.get().getFieldType());
        Assertions.assertEquals(
                "_.metadata.parent_document_id", column.get().getFieldPath().getFieldPath());

        column = columns.stream()
                .filter(columnInfo -> Objects.equals(columnInfo.getColumnName(), "document_id"))
                .findFirst();
        Assertions.assertTrue(column.isPresent());
        Assertions.assertEquals("document_id", column.get().getColumnLabel());
        Assertions.assertEquals(WDFieldType.STRING, column.get().getFieldType());
        Assertions.assertEquals("_.document_id", column.get().getFieldPath().getFieldPath());

        column = columns.stream()
                .filter(columnInfo -> Objects.equals(columnInfo.getColumnName(), "wd_field_path"))
                .findFirst();
        Assertions.assertTrue(column.isPresent());
        Assertions.assertEquals("wd_field_path", column.get().getColumnLabel());
        Assertions.assertEquals(WDFieldType.STRING, column.get().getFieldType());
        Assertions.assertEquals("_.", column.get().getFieldPath().getFieldPath());

        column = columns.stream()
                .filter(columnInfo -> Objects.equals(
                        columnInfo.getColumnName(), "enriched_html_wd_nested_seq_num"))
                .findFirst();
        Assertions.assertTrue(column.isPresent());
        Assertions.assertEquals("enriched_html_wd_nested_seq_num", column.get().getColumnLabel());
        Assertions.assertEquals(WDFieldType.LONG, column.get().getFieldType());
        Assertions.assertEquals(
                "_.enriched_html.wd_nested_seq_num", column.get().getFieldPath().getFieldPath());

        column = columns.stream()
                .filter(columnInfo -> Objects.equals(
                        columnInfo.getColumnName(), "enriched_html_tables_wd_nested_seq_num"))
                .findFirst();
        Assertions.assertTrue(column.isPresent());
        Assertions.assertEquals(
                "enriched_html_tables_wd_nested_seq_num", column.get().getColumnLabel());
        Assertions.assertEquals(WDFieldType.LONG, column.get().getFieldType());
        Assertions.assertEquals(
                "_.enriched_html.tables.wd_nested_seq_num",
                column.get().getFieldPath().getFieldPath());

        column = columns.stream()
                .filter(columnInfo -> Objects.equals(
                        columnInfo.getColumnName(),
                        "enriched_html_tables_body_cells_wd_nested_seq_num"))
                .findFirst();
        Assertions.assertTrue(column.isPresent());
        Assertions.assertEquals(
                "enriched_html_tables_body_cells_wd_nested_seq_num",
                column.get().getColumnLabel());
        Assertions.assertEquals(WDFieldType.LONG, column.get().getFieldType());
        Assertions.assertEquals(
                "_.enriched_html.tables.body_cells.wd_nested_seq_num",
                column.get().getFieldPath().getFieldPath());

        column = columns.stream()
                .filter(columnInfo -> Objects.equals(
                        columnInfo.getColumnName(), "enriched_html_tables_body_cells_cell_id"))
                .findFirst();
        Assertions.assertTrue(column.isPresent());
        Assertions.assertEquals(
                "enriched_html_tables_body_cells_cell_id", column.get().getColumnLabel());
        Assertions.assertEquals(WDFieldType.STRING, column.get().getFieldType());
        Assertions.assertEquals(
                "_.enriched_html.tables.body_cells.cell_id",
                column.get().getFieldPath().getFieldPath());

        table = schemaInfoList.stream()
                .filter(schemaInfo -> Objects.equals(
                        schemaInfo.getTableName(),
                        "Annual Report Collection 1 [enriched_html.contract.elements.location]"))
                .findFirst();
        Assertions.assertTrue(table.isPresent());
        Assertions.assertEquals("Annual Report Collection 1", table.get().getCollectionName());
        Assertions.assertEquals(
                "8357c1c6-1742-24c1-0000-018d1123e258", table.get().getCollectionId());
        Assertions.assertEquals(
                "_.enriched_html.contract.elements.location",
                table.get().getFieldPathToDoc().getFieldPath());
        Assertions.assertNull(table.get().getFieldPathToDoc().getOptionalEnrichFieldInfo());

        columns = table.get().getColumns();
        Assertions.assertEquals(9, columns.size());
        column = columns.stream()
                .filter(columnInfo ->
                        Objects.equals(columnInfo.getColumnName(), "parent_document_id"))
                .findFirst();
        Assertions.assertTrue(column.isPresent());
        Assertions.assertEquals("parent_document_id", column.get().getColumnLabel());
        Assertions.assertEquals(WDFieldType.STRING, column.get().getFieldType());
        Assertions.assertEquals(
                "_.metadata.parent_document_id", column.get().getFieldPath().getFieldPath());

        column = columns.stream()
                .filter(columnInfo -> Objects.equals(columnInfo.getColumnName(), "document_id"))
                .findFirst();
        Assertions.assertTrue(column.isPresent());
        Assertions.assertEquals("document_id", column.get().getColumnLabel());
        Assertions.assertEquals(WDFieldType.STRING, column.get().getFieldType());
        Assertions.assertEquals("_.document_id", column.get().getFieldPath().getFieldPath());

        column = columns.stream()
                .filter(columnInfo -> Objects.equals(columnInfo.getColumnName(), "wd_field_path"))
                .findFirst();
        Assertions.assertTrue(column.isPresent());
        Assertions.assertEquals("wd_field_path", column.get().getColumnLabel());
        Assertions.assertEquals(WDFieldType.STRING, column.get().getFieldType());
        Assertions.assertEquals("_.", column.get().getFieldPath().getFieldPath());

        column = columns.stream()
                .filter(columnInfo -> Objects.equals(
                        columnInfo.getColumnName(), "enriched_html_wd_nested_seq_num"))
                .findFirst();
        Assertions.assertTrue(column.isPresent());
        Assertions.assertEquals("enriched_html_wd_nested_seq_num", column.get().getColumnLabel());
        Assertions.assertEquals(WDFieldType.LONG, column.get().getFieldType());
        Assertions.assertEquals(
                "_.enriched_html.wd_nested_seq_num", column.get().getFieldPath().getFieldPath());

        column = columns.stream()
                .filter(columnInfo -> Objects.equals(
                        columnInfo.getColumnName(), "enriched_html_contract_wd_nested_seq_num"))
                .findFirst();
        Assertions.assertTrue(column.isPresent());
        Assertions.assertEquals(
                "enriched_html_contract_wd_nested_seq_num", column.get().getColumnLabel());
        Assertions.assertEquals(WDFieldType.LONG, column.get().getFieldType());
        Assertions.assertEquals(
                "_.enriched_html.contract.wd_nested_seq_num",
                column.get().getFieldPath().getFieldPath());

        column = columns.stream()
                .filter(columnInfo -> Objects.equals(
                        columnInfo.getColumnName(),
                        "enriched_html_contract_elements_wd_nested_seq_num"))
                .findFirst();
        Assertions.assertTrue(column.isPresent());
        Assertions.assertEquals(
                "enriched_html_contract_elements_wd_nested_seq_num",
                column.get().getColumnLabel());
        Assertions.assertEquals(WDFieldType.LONG, column.get().getFieldType());
        Assertions.assertEquals(
                "_.enriched_html.contract.elements.wd_nested_seq_num",
                column.get().getFieldPath().getFieldPath());

        column = columns.stream()
                .filter(columnInfo -> Objects.equals(
                        columnInfo.getColumnName(),
                        "enriched_html_contract_elements_location_wd_nested_seq_num"))
                .findFirst();
        Assertions.assertTrue(column.isPresent());
        Assertions.assertEquals(
                "enriched_html_contract_elements_location_wd_nested_seq_num",
                column.get().getColumnLabel());
        Assertions.assertEquals(WDFieldType.LONG, column.get().getFieldType());
        Assertions.assertEquals(
                "_.enriched_html.contract.elements.location.wd_nested_seq_num",
                column.get().getFieldPath().getFieldPath());

        column = columns.stream()
                .filter(columnInfo -> Objects.equals(
                        columnInfo.getColumnName(),
                        "enriched_html_contract_elements_location_begin"))
                .findFirst();
        Assertions.assertTrue(column.isPresent());
        Assertions.assertEquals(
                "enriched_html_contract_elements_location_begin", column.get().getColumnLabel());
        Assertions.assertEquals(WDFieldType.LONG, column.get().getFieldType());
        Assertions.assertEquals(
                "_.enriched_html.contract.elements.location.begin",
                column.get().getFieldPath().getFieldPath());
    }

    @Test
    public void testFetchTableInfoOfNestedField() throws SQLException, IOException {

        Properties properties = new Properties();
        WDInfoStore infoStore = getInfoStore(properties);
        List<WDTableInfo> schemaInfoList = infoStore.fetchTableInfoList(
                "e6402476-1bbb-4971-9839-c062e593a7b3", "8357c1c6-1742-24c1-0000-018d1123e258");

        Assertions.assertEquals(22, schemaInfoList.size());

        Optional<WDTableInfo> table = schemaInfoList.stream()
                .filter(schemaInfo -> Objects.equals(
                        schemaInfo.getTableName(), "Annual Report Collection 1 [metadata]"))
                .findFirst();
        Assertions.assertTrue(table.isPresent());
        Assertions.assertEquals("Annual Report Collection 1", table.get().getCollectionName());
        Assertions.assertEquals(
                "8357c1c6-1742-24c1-0000-018d1123e258", table.get().getCollectionId());
        Assertions.assertEquals("_.metadata", table.get().getFieldPathToDoc().getFieldPath());
        Assertions.assertNull(table.get().getFieldPathToDoc().getOptionalEnrichFieldInfo());

        List<WDColumnInfo> columns = table.get().getColumns();
        Assertions.assertEquals(5, columns.size());
        Optional<WDColumnInfo> column = columns.stream()
                .filter(columnInfo ->
                        Objects.equals(columnInfo.getColumnName(), "parent_document_id"))
                .findFirst();
        Assertions.assertTrue(column.isPresent());
        Assertions.assertEquals("parent_document_id", column.get().getColumnLabel());
        Assertions.assertEquals(WDFieldType.STRING, column.get().getFieldType());
        Assertions.assertEquals(
                "_.metadata.parent_document_id", column.get().getFieldPath().getFieldPath());

        column = columns.stream()
                .filter(columnInfo -> Objects.equals(columnInfo.getColumnName(), "document_id"))
                .findFirst();
        Assertions.assertTrue(column.isPresent());
        Assertions.assertEquals("document_id", column.get().getColumnLabel());
        Assertions.assertEquals(WDFieldType.STRING, column.get().getFieldType());
        Assertions.assertEquals("_.document_id", column.get().getFieldPath().getFieldPath());

        column = columns.stream()
                .filter(columnInfo -> Objects.equals(columnInfo.getColumnName(), "wd_field_path"))
                .findFirst();
        Assertions.assertTrue(column.isPresent());
        Assertions.assertEquals("wd_field_path", column.get().getColumnLabel());
        Assertions.assertEquals(WDFieldType.STRING, column.get().getFieldType());
        Assertions.assertEquals("_.", column.get().getFieldPath().getFieldPath());

        column = columns.stream()
                .filter(columnInfo ->
                        Objects.equals(columnInfo.getColumnName(), "metadata_wd_nested_seq_num"))
                .findFirst();
        Assertions.assertTrue(column.isPresent());
        Assertions.assertEquals("metadata_wd_nested_seq_num", column.get().getColumnLabel());
        Assertions.assertEquals(WDFieldType.LONG, column.get().getFieldType());
        Assertions.assertEquals(
                "_.metadata.wd_nested_seq_num", column.get().getFieldPath().getFieldPath());

        column = columns.stream()
                .filter(columnInfo ->
                        Objects.equals(columnInfo.getColumnName(), "metadata_customer_id"))
                .findFirst();
        Assertions.assertTrue(column.isPresent());
        Assertions.assertEquals("metadata_customer_id", column.get().getColumnLabel());
        Assertions.assertEquals(WDFieldType.STRING, column.get().getFieldType());
        Assertions.assertEquals(
                "_.metadata.customer_id", column.get().getFieldPath().getFieldPath());
    }

    @Test
    public void testFetchTableInfoOfExtractedMetadata() throws SQLException, IOException {

        Properties properties = new Properties();
        WDInfoStore infoStore = getInfoStore(properties);
        List<WDTableInfo> schemaInfoList = infoStore.fetchTableInfoList(
                "e6402476-1bbb-4971-9839-c062e593a7b3", "8357c1c6-1742-24c1-0000-018d1123e258");

        Assertions.assertEquals(22, schemaInfoList.size());

        Optional<WDTableInfo> table = schemaInfoList.stream()
                .filter(schemaInfo -> Objects.equals(
                        schemaInfo.getTableName(),
                        "Annual Report Collection 1 [extracted_metadata]"))
                .findFirst();
        Assertions.assertTrue(table.isPresent());
        Assertions.assertEquals("Annual Report Collection 1", table.get().getCollectionName());
        Assertions.assertEquals(
                "8357c1c6-1742-24c1-0000-018d1123e258", table.get().getCollectionId());
        Assertions.assertEquals(
                "_.extracted_metadata", table.get().getFieldPathToDoc().getFieldPath());
        Assertions.assertNull(table.get().getFieldPathToDoc().getOptionalEnrichFieldInfo());

        List<WDColumnInfo> columns = table.get().getColumns();
        Assertions.assertEquals(11, columns.size());
        Optional<WDColumnInfo> column = columns.stream()
                .filter(columnInfo ->
                        Objects.equals(columnInfo.getColumnName(), "parent_document_id"))
                .findFirst();
        Assertions.assertTrue(column.isPresent());
        Assertions.assertEquals("parent_document_id", column.get().getColumnLabel());
        Assertions.assertEquals(WDFieldType.STRING, column.get().getFieldType());
        Assertions.assertEquals(
                "_.metadata.parent_document_id", column.get().getFieldPath().getFieldPath());

        column = columns.stream()
                .filter(columnInfo -> Objects.equals(columnInfo.getColumnName(), "document_id"))
                .findFirst();
        Assertions.assertTrue(column.isPresent());
        Assertions.assertEquals("document_id", column.get().getColumnLabel());
        Assertions.assertEquals(WDFieldType.STRING, column.get().getFieldType());
        Assertions.assertEquals("_.document_id", column.get().getFieldPath().getFieldPath());

        column = columns.stream()
                .filter(columnInfo -> Objects.equals(columnInfo.getColumnName(), "wd_field_path"))
                .findFirst();
        Assertions.assertTrue(column.isPresent());
        Assertions.assertEquals("wd_field_path", column.get().getColumnLabel());
        Assertions.assertEquals(WDFieldType.STRING, column.get().getFieldType());
        Assertions.assertEquals("_.", column.get().getFieldPath().getFieldPath());

        column = columns.stream()
                .filter(columnInfo -> Objects.equals(
                        columnInfo.getColumnName(), "extracted_metadata_wd_nested_seq_num"))
                .findFirst();
        Assertions.assertTrue(column.isPresent());
        Assertions.assertEquals(
                "extracted_metadata_wd_nested_seq_num", column.get().getColumnLabel());
        Assertions.assertEquals(WDFieldType.LONG, column.get().getFieldType());
        Assertions.assertEquals(
                "_.extracted_metadata.wd_nested_seq_num",
                column.get().getFieldPath().getFieldPath());

        column = columns.stream()
                .filter(columnInfo ->
                        Objects.equals(columnInfo.getColumnName(), "extracted_metadata_author"))
                .findFirst();
        Assertions.assertTrue(column.isPresent());
        Assertions.assertEquals("extracted_metadata_author", column.get().getColumnLabel());
        Assertions.assertEquals(WDFieldType.STRING, column.get().getFieldType());
        Assertions.assertEquals(
                "_.extracted_metadata.author", column.get().getFieldPath().getFieldPath());
    }

    @Test
    public void testFetchTableInfoOfRoot() throws SQLException, IOException {

        Properties properties = new Properties();
        WDInfoStore infoStore = getInfoStore(properties);
        List<WDTableInfo> schemaInfoList = infoStore.fetchTableInfoList(
                "e6402476-1bbb-4971-9839-c062e593a7b3", "8357c1c6-1742-24c1-0000-018d1123e258");

        Assertions.assertEquals(22, schemaInfoList.size());

        Optional<WDTableInfo> table = schemaInfoList.stream()
                .filter(schemaInfo -> Objects.equals(
                        schemaInfo.getTableName(), "Annual Report Collection 1 [wd_root_doc]"))
                .findFirst();
        Assertions.assertTrue(table.isPresent());
        Assertions.assertEquals("Annual Report Collection 1", table.get().getCollectionName());
        Assertions.assertEquals(
                "8357c1c6-1742-24c1-0000-018d1123e258", table.get().getCollectionId());
        Assertions.assertEquals("_", table.get().getFieldPathToDoc().getFieldPath());
        Assertions.assertNull(table.get().getFieldPathToDoc().getOptionalEnrichFieldInfo());

        List<WDColumnInfo> columns = table.get().getColumns();
        Assertions.assertEquals(4, columns.size());
        Optional<WDColumnInfo> column = columns.stream()
                .filter(columnInfo ->
                        Objects.equals(columnInfo.getColumnName(), "parent_document_id"))
                .findFirst();
        Assertions.assertTrue(column.isPresent());
        Assertions.assertEquals("parent_document_id", column.get().getColumnLabel());
        Assertions.assertEquals(WDFieldType.STRING, column.get().getFieldType());
        Assertions.assertEquals(
                "_.metadata.parent_document_id", column.get().getFieldPath().getFieldPath());

        column = columns.stream()
                .filter(columnInfo -> Objects.equals(columnInfo.getColumnName(), "document_id"))
                .findFirst();
        Assertions.assertTrue(column.isPresent());
        Assertions.assertEquals("document_id", column.get().getColumnLabel());
        Assertions.assertEquals(WDFieldType.STRING, column.get().getFieldType());
        Assertions.assertEquals("_.document_id", column.get().getFieldPath().getFieldPath());

        column = columns.stream()
                .filter(columnInfo -> Objects.equals(columnInfo.getColumnName(), "wd_field_path"))
                .findFirst();
        Assertions.assertTrue(column.isPresent());
        Assertions.assertEquals("wd_field_path", column.get().getColumnLabel());
        Assertions.assertEquals(WDFieldType.STRING, column.get().getFieldType());
        Assertions.assertEquals("_.", column.get().getFieldPath().getFieldPath());

        column = columns.stream()
                .filter(columnInfo -> Objects.equals(columnInfo.getColumnName(), "text"))
                .findFirst();
        Assertions.assertTrue(column.isPresent());
        Assertions.assertEquals("text", column.get().getColumnLabel());
        Assertions.assertEquals(WDFieldType.STRING, column.get().getFieldType());
        Assertions.assertEquals("_.text", column.get().getFieldPath().getFieldPath());
    }

    @Test
    public void testFetchTableInfoListFromMultipleCollections() throws SQLException, IOException {

        Properties properties = new Properties();
        WDInfoStore infoStore = getInfoStore(properties);
        List<WDTableInfo> schemaInfoList =
                infoStore.fetchTableInfoList("e6402476-1bbb-4971-9839-c062e593a7b3");

        Assertions.assertEquals(44, schemaInfoList.size());

        Optional<WDTableInfo> table = schemaInfoList.stream()
                .filter(schemaInfo -> Objects.equals(
                        schemaInfo.getTableName(), "Annual Report Collection 1 [wd_root_doc]"))
                .findFirst();
        Assertions.assertTrue(table.isPresent());
        Assertions.assertEquals("Annual Report Collection 1", table.get().getCollectionName());
        Assertions.assertEquals(
                "8357c1c6-1742-24c1-0000-018d1123e258", table.get().getCollectionId());
        Assertions.assertEquals("_", table.get().getFieldPathToDoc().getFieldPath());
        Assertions.assertNull(table.get().getFieldPathToDoc().getOptionalEnrichFieldInfo());

        table = schemaInfoList.stream()
                .filter(schemaInfo -> Objects.equals(
                        schemaInfo.getTableName(), "Annual Report Collection 2 [wd_root_doc]"))
                .findFirst();
        Assertions.assertTrue(table.isPresent());
        Assertions.assertEquals("Annual Report Collection 2", table.get().getCollectionName());
        Assertions.assertEquals(
                "7357c1c6-1742-24c1-0000-018d1123e258", table.get().getCollectionId());
        Assertions.assertEquals("_", table.get().getFieldPathToDoc().getFieldPath());
        Assertions.assertNull(table.get().getFieldPathToDoc().getOptionalEnrichFieldInfo());
    }
}
