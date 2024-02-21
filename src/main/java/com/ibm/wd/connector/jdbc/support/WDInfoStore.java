package com.ibm.wd.connector.jdbc.support;

import static com.ibm.wd.connector.jdbc.WDProperties.WD_COLUMN_SEPARATOR;
import static com.ibm.wd.connector.jdbc.WDProperties.WD_GENERATE_SUB_TABLES_STRICTLY;
import static com.ibm.wd.connector.jdbc.model.WDConstants.*;
import static com.ibm.wd.connector.jdbc.model.WDFieldPath.*;

import com.ibm.watson.discovery.v2.Discovery;
import com.ibm.watson.discovery.v2.model.*;
import com.ibm.watson.discovery.v2.model.Collection;
import com.ibm.wd.connector.jdbc.WDProperties;
import com.ibm.wd.connector.jdbc.model.*;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class WDInfoStore implements WDInfoStoreInterface {

    private final WDClientInterface client;
    private final Properties properties;

    public WDInfoStore(Discovery discovery, Properties properties) {
        this.client = new WDSimpleClient(discovery);
        this.properties = properties;
    }

    @Override
    public List<WDSchemaInfo> fetchSchemaInfoList() throws SQLException {
        String projectIds = WDProperties.WD_PROJECT_IDS_TO_LIST_SCHEMA.get(properties);
        Stream<ProjectListDetails> projects = client.listProjects().stream();
        if (projectIds != null && !StringUtils.isBlank(projectIds)) {
            Set<String> allowList = new HashSet<>(Arrays.asList(projectIds.split(",")));
            if (!allowList.isEmpty()) {
                projects = projects.filter(project -> allowList.contains(project.getProjectId()));
            }
        }
        return projects.map(this::convertToSchemaInfo).collect(Collectors.toList());
    }

    @Override
    public List<WDTableInfo> fetchTableInfoList(String projectId) throws SQLException {
        List<Field> fields = client.listFields(projectId);
        List<WDTableInfo> tableInfoList = new ArrayList<>();
        for (Collection c : client.listCollections(projectId)) {
            tableInfoList.addAll(fetchTableInfoList(projectId, c.getCollectionId(), fields));
        }
        return tableInfoList;
    }

    @Override
    public List<WDTableInfo> fetchTableInfoList(String projectId, String collectionId)
            throws SQLException {
        List<Field> fields = client.listFields(projectId);
        return fetchTableInfoList(projectId, collectionId, fields);
    }

    private List<WDTableInfo> fetchTableInfoList(
            String projectId, String collectionId, List<Field> fields) throws SQLException {
        List<WDTableInfo> tableInfoList = new ArrayList<>();
        final boolean generateSubTablesStrictly =
                Boolean.parseBoolean(WD_GENERATE_SUB_TABLES_STRICTLY.get(properties));

        CollectionDetails collection = client.getCollection(projectId, collectionId);
        WDTableInfo.Builder baseBuilder = convertToTableInfoBuilder(collection);

        List<WDColumnInfo> basicColumns = Arrays.asList(
                new WDColumnInfo.Builder()
                        .columnName("parent_document_id")
                        .columnLabel("parent_document_id")
                        .collectionId(collectionId)
                        .fieldPath(new WDFieldPath(
                                "parent_document_id", DISCOVERY_PARENT_DOCUMENT_ID_FIELD_PATH))
                        .fieldType(WDFieldType.STRING)
                        .isPrimary(true)
                        .build(),
                new WDColumnInfo.Builder()
                        .columnName("document_id")
                        .columnLabel("document_id")
                        .collectionId(collectionId)
                        .fieldPath(new WDFieldPath("document_id", DISCOVERY_DOCUMENT_ID_FIELD_PATH))
                        .fieldType(WDFieldType.STRING)
                        .isPrimary(true)
                        .build(),
                // special column info for field path to record
                new WDColumnInfo.Builder()
                        .columnName(FIELD_PATH_PATH.getLabel())
                        .columnLabel(FIELD_PATH_PATH.getLabel())
                        .collectionId(collectionId)
                        .fieldPath(FIELD_PATH_PATH)
                        .fieldType(WDFieldType.STRING)
                        .isPrimary(true)
                        .build());

        if (!generateSubTablesStrictly) {
            // special column info for sequence
            basicColumns.add(new WDColumnInfo.Builder()
                    .columnName(RECORD_SEQ_PATH.getLabel())
                    .columnLabel(RECORD_SEQ_PATH.getLabel())
                    .collectionId(collectionId)
                    .fieldPath(RECORD_SEQ_PATH)
                    .fieldType(WDFieldType.LONG)
                    .isPrimary(true)
                    .build());
        }

        fields = fields.stream()
                .filter(f -> collectionId.equals(f.getCollectionId()))
                .map(f -> new InternalField(f.getField(), f.getType(), f.getCollectionId()))
                .collect(Collectors.toList());

        List<Field> nestedFields = new ArrayList<>();
        EnumMap<WDEnrichedFieldEnum, Map<String, List<Field>>> fieldsOfEnriched =
                new EnumMap<>(WDEnrichedFieldEnum.class);

        for (Field field : fields) {
            WDEnrichedFieldEnum fieldEnum =
                    WDEnrichedFieldEnum.findEnrichedFieldEnum(field.getField());
            if (fieldEnum != null) {
                String topFieldName =
                        field.getField().split(DISCOVERY_FIELD_PATH_SEPARATOR_REGEX)[0];
                fieldsOfEnriched
                        .computeIfAbsent(fieldEnum, (k) -> new HashMap<>())
                        .computeIfAbsent(topFieldName, (k) -> new ArrayList<>())
                        .add(field);
            } else {
                nestedFields.add(field);
            }
        }
        nestedFields = findNestedFields(nestedFields, collectionId);

        // sub table for enriched fields

        for (Map.Entry<WDEnrichedFieldEnum, Map<String, List<Field>>> entry :
                fieldsOfEnriched.entrySet()) {
            WDEnrichedFieldEnum fieldEnum = entry.getKey();
            Map<String, List<Field>> groupedEnrichedFields = entry.getValue();
            for (Map.Entry<String, List<Field>> enrichedFields : groupedEnrichedFields.entrySet()) {
                baseBuilder.replaceColumnInfoWith(basicColumns);
                String enrichTargetField = enrichedFields.getKey();
                WDFieldPath rootEnrichedFieldPath = new WDFieldPath(
                        fieldEnum.formattedLabel(enrichTargetField),
                        fieldEnum.formattedFieldPath(enrichTargetField),
                        fieldEnum);

                if (generateSubTablesStrictly) {
                    generateNestedSeqNumColumns(rootEnrichedFieldPath).forEach((cb) -> {
                        baseBuilder.addColumn(cb.collectionId(collectionId).build());
                    });
                }

                for (Field enrichedField : enrichedFields.getValue()) {
                    fields.remove(enrichedField);
                    if (Field.Type.NESTED.equals(enrichedField.getType())) {
                        continue;
                    }
                    if (rootEnrichedFieldPath.getFieldPath().equals(enrichedField.getField())) {
                        continue;
                    }
                    if (fieldEnum == WDEnrichedFieldEnum.TABLES) {
                        if (!enrichedField.getField().contains("body_cells")) {
                            continue;
                        }
                        if (enrichedField.getField().contains("attributes")) {
                            continue;
                        }
                    }
                    WDColumnInfo.Builder ciBuilder =
                            convertToColumnInfoBuilder(enrichedField, rootEnrichedFieldPath);
                    ciBuilder.fieldPath(convertToFieldPath(enrichedField));
                    baseBuilder.addColumn(ciBuilder.build());
                }

                baseBuilder.fieldPathToDoc(rootEnrichedFieldPath);
                tableInfoList.add(baseBuilder.build());
            }
        }

        // sub table for nested fields

        nestedFields = nestedFields.stream()
                .map(f -> new ImmutablePair<>(
                        Arrays.asList(f.getField().split(DISCOVERY_FIELD_PATH_SEPARATOR_REGEX)), f))
                .sorted((f1, f2) -> {
                    List<String> comp1 = f1.getKey();
                    List<String> comp2 = f2.getKey();
                    if (comp1.size() == comp2.size()) {
                        for (int i = 0; i < comp1.size(); i++) {
                            int compared = comp2.get(i).compareTo(comp1.get(i));
                            if (compared != 0) {
                                return compared;
                            }
                        }
                        return 0;
                    } else {
                        return comp2.size() - comp1.size();
                    }
                })
                .map(ImmutablePair::getValue)
                .collect(Collectors.toList());

        for (Field nestedField : nestedFields) {
            fields.remove(nestedField);
            baseBuilder.replaceColumnInfoWith(basicColumns);

            List<Field> groupedFields = groupByTopNestedField(fields, nestedField.getField());
            if (groupedFields.size() == 0) {
                continue;
            }
            WDFieldPath nestedDocPath = convertToFieldPath(nestedField);
            if (generateSubTablesStrictly) {
                generateNestedSeqNumColumns(nestedDocPath).forEach((cb) -> {
                    baseBuilder.addColumn(cb.collectionId(collectionId).build());
                });
            }
            for (Field groupedField : groupedFields) {
                fields.remove(groupedField);
                if (groupedField.getField().equals(DISCOVERY_PARENT_DOCUMENT_ID_FIELD_PATH)) {
                    continue;
                }
                WDColumnInfo.Builder ciBuilder =
                        convertToColumnInfoBuilder(groupedField, nestedDocPath);
                ciBuilder.fieldPath(convertToFieldPath(groupedField));
                baseBuilder.addColumn(ciBuilder.build());
            }
            baseBuilder.fieldPathToDoc(nestedDocPath);

            tableInfoList.add(baseBuilder.build());
        }

        // table for root document

        baseBuilder.replaceColumnInfoWith(basicColumns);
        baseBuilder.fieldPathToDoc(WDFieldPath.ROOT_DOC_PATH);
        for (Field field : fields) {
            if (field.getField().equals(DISCOVERY_DOCUMENT_ID_FIELD_PATH)) {
                continue;
            }
            if (field.getField().startsWith(DISCOVERY_ENRICHED_FIELD_PREFIX)) {
                continue;
            }
            if (field.getField().equals(DISCOVERY_DOCUMENT_LEVEL_ENRICHED_FIELD_NAME)) {
                continue;
            }
            WDColumnInfo.Builder ciBuilder =
                    convertToColumnInfoBuilder(field, WDFieldPath.ROOT_DOC_PATH);
            baseBuilder.addColumn(ciBuilder.build());
        }
        tableInfoList.add(baseBuilder.build());

        return tableInfoList;
    }

    private static class InternalField extends Field {
        private InternalField(String field, String type, String collectionId) {
            super();
            this.field = field;
            this.type = type;
            this.collectionId = collectionId;
        }

        @Override
        public boolean equals(Object other) {
            if (other == null) return false;
            if (other == this) return true;
            if (!(other instanceof Field)) return false;
            Field otherField = (Field) other;
            return Objects.equals(field, otherField.getField())
                    && Objects.equals(collectionId, otherField.getCollectionId())
                    && Objects.equals(type, otherField.getType());
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.field, this.collectionId, this.type);
        }
    }

    private List<Field> findNestedFields(List<Field> candidateOfNestedFields, String collectionId) {

        Set<String> registered = new HashSet<>();
        for (Field field : candidateOfNestedFields) {
            if (Field.Type.NESTED.equals(field.getType())) continue;
            String fieldPath = field.getField();
            int found = fieldPath.lastIndexOf('.');
            if (found < 0) continue;
            String parentFieldPath = fieldPath.substring(0, found + 1);
            registered.add(parentFieldPath);
        }
        Map<String, Field> generated = new HashMap<>();
        for (String fieldPath : registered) {
            List<String> pathComps =
                    Arrays.asList(fieldPath.split(DISCOVERY_FIELD_PATH_SEPARATOR_REGEX));
            for (int compIndex = pathComps.size(); 0 < compIndex; compIndex--) {
                String path = String.join(
                        DISCOVERY_FIELD_PATH_SEPARATOR, pathComps.subList(0, compIndex));
                if (generated.containsKey(path)) {
                    break;
                }
                generated.put(path, new InternalField(path, Field.Type.NESTED, collectionId));
            }
        }
        return new ArrayList<>(generated.values());
    }

    private List<Field> groupByTopNestedField(List<Field> fields, String topNestedFieldPrefix) {
        List<Field> fieldsUnderTop = fields.stream()
                .filter(f -> f.getField().startsWith(topNestedFieldPrefix))
                .collect(Collectors.toList());

        List<Field> nestedFieldsUnderTop = fieldsUnderTop.stream()
                .filter(f -> f.getType().equals(Field.Type.NESTED))
                .collect(Collectors.toList());

        List<String> secondTopNestedFieldPrefixesUnderTop = nestedFieldsUnderTop.stream()
                .filter(f1 -> nestedFieldsUnderTop.stream()
                                .filter(f2 -> f1.getField().startsWith(f2.getField()))
                                .count()
                        == 1)
                .map(f -> f.getField() + DISCOVERY_FIELD_PATH_SEPARATOR)
                .collect(Collectors.toList());

        return fieldsUnderTop.stream()
                .filter(field -> secondTopNestedFieldPrefixesUnderTop.stream()
                        .noneMatch(prefix -> field.getField().startsWith(prefix)))
                .collect(Collectors.toList());
    }

    private WDSchemaInfo convertToSchemaInfo(ProjectListDetails project) {
        return new WDSchemaInfo(project.getName(), project.getProjectId());
    }

    private WDFieldPath convertToFieldPath(Field field) {
        return new WDFieldPath(field.getField(), field.getField());
    }

    private WDColumnInfo.Builder convertToColumnInfoBuilder(
            Field field, WDFieldPath forNestedField) {
        String columnName = columnNameFromFieldPath(field.getField());
        return new WDColumnInfo.Builder()
                .columnName(columnName)
                .columnLabel(columnName)
                .fieldType(
                        field.getField().equals(DISCOVERY_HTML_FIELD_PATH)
                                ? WDFieldType.HTML
                                : WDFieldType.resolveByWDType(field.getType()))
                .fieldPath(new WDFieldPath(field.getField(), field.getField()))
                .collectionId(field.getCollectionId())
                .isPrimary(false);
    }

    private String columnNameFromFieldPath(String fieldPathStr) {
        if (WD_COLUMN_SEPARATOR.get(properties).equals(DISCOVERY_FIELD_PATH_SEPARATOR)) {
            return fieldPathStr;
        } else {
            return fieldPathStr.replace(
                    DISCOVERY_FIELD_PATH_SEPARATOR, WD_COLUMN_SEPARATOR.get(properties));
        }
    }

    private List<WDColumnInfo.Builder> generateNestedSeqNumColumns(WDFieldPath forNestedField) {
        final String[] rootFieldPathComps = forNestedField.getFieldPathComps();
        List<WDColumnInfo.Builder> columnInfoList = new ArrayList<>();
        // to skip root, since we already have document_id, parent_document_id
        for (int i = 1; i < rootFieldPathComps.length; i++) {
            WDFieldPath seqNumFieldPath = getNestedSeqNumFieldPath(rootFieldPathComps, i);
            String columnName =
                    columnNameFromFieldPath(ROOT_DOC_PATH.subFieldPath(seqNumFieldPath, true));
            columnInfoList.add(new WDColumnInfo.Builder()
                    .columnName(columnName)
                    .columnLabel(columnName)
                    .fieldPath(seqNumFieldPath)
                    .fieldType(WDFieldType.LONG)
                    .isPrimary(true));
        }
        return columnInfoList;
    }

    private WDTableInfo.Builder convertToTableInfoBuilder(CollectionDetails collection) {
        return new WDTableInfo.Builder()
                .collectionName(collection.name())
                .collectionId(collection.collectionId())
                .description(collection.description());
    }

    private static class CachedWDSimpleClient implements WDClientInterface {

        private WDSimpleClient client;

        private List<ProjectListDetails> projects;

        private final Map<String, List<Field>> fields;

        private final Map<String, List<Collection>> collections;

        private final Map<String, Map<String, CollectionDetails>> collectionDetails;

        private final Map<String, List<Enrichment>> enrichments;

        public CachedWDSimpleClient(WDSimpleClient client) {
            this.client = client;
            this.fields = new HashMap<>();
            this.collections = new HashMap<>();
            this.collectionDetails = new HashMap<>();
            this.enrichments = new HashMap<>();
        }

        public List<ProjectListDetails> listProjects() throws SQLException {
            if (projects == null) {
                projects = client.listProjects();
            }
            return projects;
        }

        public List<Field> listFields(String projectId) throws SQLException {
            if (!fields.containsKey(projectId)) {
                fields.put(projectId, client.listFields(projectId));
            }
            return fields.get(projectId);
        }

        public List<Collection> listCollections(String projectId) throws SQLException {
            if (!collections.containsKey(projectId)) {
                collections.put(projectId, client.listCollections(projectId));
            }
            return collections.get(projectId);
        }

        public CollectionDetails getCollection(String projectId, String collectionId)
                throws SQLException {
            Map<String, CollectionDetails> details =
                    collectionDetails.computeIfAbsent(projectId, (key) -> new HashMap<>());
            if (!details.containsKey(projectId)) {
                details.put(projectId, client.getCollection(projectId, collectionId));
            }
            return details.get(collectionId);
        }
    }
}
