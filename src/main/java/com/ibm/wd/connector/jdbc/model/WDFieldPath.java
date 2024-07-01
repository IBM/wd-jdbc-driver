package com.ibm.wd.connector.jdbc.model;

import static com.ibm.wd.connector.jdbc.model.WDConstants.*;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WDFieldPath {

    private static final String ROOT_DOT_PATH_NAME = "_";
    private static final String DISCOVERY_NESTED_SEQUENCE_NUMBER_PATH_NAME = "wd_nested_seq_num";
    public static final String DISCOVERY_ENRICHED_FIELD_PREFIX = "enriched_";
    public static final String DISCOVERY_DOCUMENT_LEVEL_ENRICHED_FIELD_NAME =
            "document_level_enrichment";
    // to tell that record is under root document
    public static final WDFieldPath ROOT_DOC_PATH =
            new WDFieldPath("wd_root_doc", ROOT_DOT_PATH_NAME);

    // placeholder to create a wd_record_seq column
    public static final WDFieldPath RECORD_SEQ_PATH = new WDFieldPath("wd_record_seq", "");
    public static final WDFieldPath FIELD_PATH_PATH = new WDFieldPath("wd_field_path", "");

    private final String label;
    private final String fieldPath;
    private final WDEnrichedFieldEnum optionalEnrichFieldInfo;

    public String getLabel() {
        return label;
    }

    public String getFieldPath() {
        return fieldPath;
    }

    public String[] getFieldPathComps() {
        return fieldPath.split(DISCOVERY_FIELD_PATH_SEPARATOR_REGEX);
    }

    public WDEnrichedFieldEnum getOptionalEnrichFieldInfo() {
        return optionalEnrichFieldInfo;
    }

    public boolean isEnrichedField() {
        return optionalEnrichFieldInfo != null;
    }

    public boolean isRootDoc() {
        return this == ROOT_DOC_PATH;
    }

    public boolean isNestedSeqNumField() {
        return fieldPath.endsWith(
                DISCOVERY_FIELD_PATH_SEPARATOR + DISCOVERY_NESTED_SEQUENCE_NUMBER_PATH_NAME);
    }

    public WDFieldPath(String label, String fieldPath) {
        this(label, fieldPath, null);
    }

    public WDFieldPath(
            String label, String fieldPath, WDEnrichedFieldEnum optionalEnrichFieldInfo) {
        this.label = label;
        if (fieldPath.startsWith(".")) {
            fieldPath = fieldPath.substring(1);
        }
        if (!fieldPath.startsWith(ROOT_DOT_PATH_NAME)) {
            fieldPath = ROOT_DOT_PATH_NAME + "." + fieldPath;
        }
        this.fieldPath = fieldPath;
        this.optionalEnrichFieldInfo = optionalEnrichFieldInfo;
    }

    public WDFieldPath copy() {
        return new WDFieldPath(label, fieldPath, optionalEnrichFieldInfo);
    }

    public String subFieldPath(WDFieldPath fieldPath) {
        return subFieldPath(fieldPath, false);
    }

    public String subFieldPath(WDFieldPath fieldPath, boolean trimSeparator) {
        if (fieldPath.getFieldPath().startsWith(this.fieldPath)) {
            if (trimSeparator) {
                String subFieldPath = fieldPath.getFieldPath().substring(this.fieldPath.length());
                if (subFieldPath.startsWith(DISCOVERY_FIELD_PATH_SEPARATOR)) {
                    subFieldPath = subFieldPath.substring(1);
                }
                return subFieldPath;
            } else {
                return fieldPath.getFieldPath().substring(this.fieldPath.length());
            }
        } else {
            return null;
        }
    }

    public static WDFieldPath getNestedSeqNumFieldPath(String[] pathComps, int until) {
        StringBuilder buffer = new StringBuilder();
        until = Math.min(until, pathComps == null ? -1 : pathComps.length - 1);
        for (int i = 0; i <= until; i++) {
            buffer.append(pathComps[i]);
            buffer.append(DISCOVERY_FIELD_PATH_SEPARATOR);
        }
        buffer.append(DISCOVERY_NESTED_SEQUENCE_NUMBER_PATH_NAME);
        final String fieldPath = buffer.toString();
        return new WDFieldPath(fieldPath, fieldPath);
    }

    public int hashCode() {
        return Objects.hash(label, fieldPath);
    }

    public boolean equals(Object o) {
        if (o == null) {
            return false;
        } else if (o == this) {
            return true;
        } else if (!(o instanceof WDFieldPath)) {
            return false;
        } else {
            WDFieldPath other = (WDFieldPath) o;
            return Objects.equals(this.label, other.label)
                    && Objects.equals(this.fieldPath, other.fieldPath)
                    && Objects.equals(this.optionalEnrichFieldInfo, other.optionalEnrichFieldInfo);
        }
    }

    public enum WDEnrichedFieldEnum {
        ENTITIES(
                "^" + DISCOVERY_ENRICHED_FIELD_PREFIX + "([^\\.]+)\\.entities.*",
                "%s" + DISCOVERY_FIELD_PATH_SEPARATOR + "entities" + DISCOVERY_FIELD_PATH_SEPARATOR
                        + "mentions",
                "entities from %s"),
        KEYWORDS(
                "^" + DISCOVERY_ENRICHED_FIELD_PREFIX + "([^\\.]+)\\.keywords.*",
                "%s" + DISCOVERY_FIELD_PATH_SEPARATOR + "keywords" + DISCOVERY_FIELD_PATH_SEPARATOR
                        + "mentions",
                "keywords from %s"),
        TABLES(
                "^" + DISCOVERY_ENRICHED_FIELD_PREFIX + "([^\\.]+)\\.tables.*",
                "%s" + DISCOVERY_FIELD_PATH_SEPARATOR + "tables" + DISCOVERY_FIELD_PATH_SEPARATOR
                        + "body_cells",
                "tables from %s"),
        TEXT_CLASSES(
                "^" + DISCOVERY_ENRICHED_FIELD_PREFIX + "([^\\.]+)\\.classes.*",
                "%s" + DISCOVERY_FIELD_PATH_SEPARATOR + "classes",
                "classes from %s"),
        DOC_CLASSES(
                "^" + DISCOVERY_DOCUMENT_LEVEL_ENRICHED_FIELD_NAME + "\\.classes.*",
                "document_level_enrichment.classes",
                "classes from document"),
    //        ADVANCED_RULES(
    //                "^enriched_([^\\.]+)\\.advanced_rules",
    //                "%s" + DISCOVERY_FIELD_PATH_SEPARATOR + "advanced_rules",
    //                "advanced rules from %s"
    //        ),
    //        RELATIONS(
    //                "^enriched_([^\\.]+)\\.relations",
    //                "%s" + DISCOVERY_FIELD_PATH_SEPARATOR + "relations",
    //                "relations from %s"
    //        ),
    ;

        private final Pattern pattern;
        private final String rootFieldPathFormat;
        private final String labelFormat;

        WDEnrichedFieldEnum(String patternStr, String rootFieldPathFormat, String labelFormat) {
            this.pattern = Pattern.compile(patternStr);
            this.rootFieldPathFormat = rootFieldPathFormat;
            this.labelFormat = labelFormat;
        }

        public static WDEnrichedFieldEnum findEnrichedFieldEnum(String enrichedFieldPath) {
            for (WDEnrichedFieldEnum fieldEnum : WDEnrichedFieldEnum.values()) {
                Matcher matcher = fieldEnum.matcher(enrichedFieldPath);
                if (matcher.matches()) {
                    return fieldEnum;
                }
            }
            return null;
        }

        public String formattedFieldPath(String appliedField) {
            return String.format(rootFieldPathFormat, appliedField);
        }

        public String formattedLabel(String appliedField) {
            return String.format(labelFormat, appliedField);
        }

        public Matcher matcher(String fieldPath) {
            return pattern.matcher(fieldPath);
        }
    }
}
