package com.ibm.wd.connector.jdbc.model;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.util.Locale;

public interface WDConstants {
    String DISCOVERY_DOCUMENT_ID_FIELD_PATH = "document_id";
    String DISCOVERY_METADATA_FIELD_PATH = "metadata";
    String DISCOVERY_PARENT_DOCUMENT_ID_FIELD_PATH = "metadata.parent_document_id";

    String DISCOVERY_HTML_FIELD_PATH = "html";

    String USERNAME_IAMAPIKEY = "iamapikey";
    String USERNAME_BEARER = "bearer";

    String DISCOVERY_FIELD_PATH_SEPARATOR = ".";
    String DISCOVERY_FIELD_PATH_SEPARATOR_REGEX = "\\.";

    DateTimeFormatter DATE_FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR, 1, 9, SignStyle.NORMAL)
            .optionalStart()
                .appendLiteral('-')
                .appendValue(ChronoField.MONTH_OF_YEAR, 1, 2, SignStyle.NOT_NEGATIVE)
                .optionalStart()
                    .appendLiteral('-')
                    .appendValue(ChronoField.DAY_OF_MONTH, 1, 2, SignStyle.NOT_NEGATIVE)
                .optionalEnd()
            .optionalEnd()
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT);

    DateTimeFormatter DATE_OPTIONAL_TIME = new DateTimeFormatterBuilder()
            .append(DATE_FORMATTER)
            .optionalStart()
                .appendLiteral('T')
                .optionalStart()
                    .appendValue(ChronoField.HOUR_OF_DAY, 1, 2, SignStyle.NOT_NEGATIVE)
                    .optionalStart()
                        .appendLiteral(':')
                        .appendValue(ChronoField.MINUTE_OF_HOUR, 1, 2, SignStyle.NOT_NEGATIVE)
                        .optionalStart()
                            .appendLiteral(':')
                            .appendValue(ChronoField.SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
                        .optionalEnd()
                        .optionalStart()
                            .appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
                        .optionalEnd()
                        .optionalStart()
                            .appendLiteral(',')
                            .appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, false)
                        .optionalEnd()
                        .optionalStart()
                            .appendZoneOrOffsetId()
                        .optionalEnd()
                        .optionalStart()
                            .appendOffset("+HHmm", "Z")
                        .optionalEnd()
                    .optionalEnd()
                .optionalEnd()
            .optionalEnd()
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT);


}
