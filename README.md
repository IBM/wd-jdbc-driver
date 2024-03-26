# Watson Discovery JDBC Driver for Cloud Pak for Data

wd-jdbc-driver allows Java programs to connect to a Watson Discovery projects, collections, and documents via JDBC driver interfaces in pure Java code. This will enable Watson Discovery to integrate with Cloud Pak for Data as data asset.

With this jdbc driver, you can list documents and the results of enrichments in Watson Discovery in a single table view as data asset.
![discovery_documents_in_data_asset](https://raw.githubusercontent.com/IBM/wd-jdbc-driver/images/images/discovery_documents_in_data_asset.png)

One of the application is to create a dashboard to list different information extracted from the same location in document.
![visualize_annotations_grouped_by_location](https://raw.githubusercontent.com/IBM/wd-jdbc-driver/images/images/visualize_annotations_grouped_by_location.png)

## Prereq

- Build with Java 17 JDK for Gradle

```shell
./gradlew generateGrammarSource
```

## Format

```shell
./gradlew spotlessApply
```

## Build

```shell
./gradlew shadowJar
```

Fat JAR file should be created in `./build/libs`. 

This jar is compatible with Java 8 JDK. (This is required by IBM Cloud Pak for Data : https://www.ibm.com/docs/en/cloud-paks/cp-data/4.8.x?topic=catalogs-generic-jdbc-connection)

## Test

```shell
./gradlew test
```

## How to use in CP4D

Before go through these steps, make sure your collection has a field that can be used for `wdCursorKeyFieldPath`.

1. Login to CP4D console.
2. Go to *Side panel -> Data -> Platform Connections*.
3. Select *JDBC drivers* tab
4. Drag and drop the JAR file built in `build/libs/` directory.
5. Select *New generic JDBC connection* button.
6. Select *Generic JDBC* connection type in *Add connection* page.
7. Fill in connection details
   1. *Name*: Any names e.g., `wd-driver`
   2. *JAR uris*: Select the JAR file you uploaded in the step above.
   3. *JDBC url*: `jdbc:wd://<WD_SERVICE_URL>`
   4. *JDBC driver*: `com.ibm.wd.connector.jdbc.WDDriver`
   5. *Username*: `bearer` for the instances in private CP4D cluster, `iamapikey` for the instances in IBM Public Cloud.
   6. *Password*: Bearer token for CP4D instance, IAM API key for IBM Cloud instances.
   7. *Properties*: You can put jdbc driver properties here. Followings would be the ones that you may set
       1. `wdCursorKeyFieldPath`: field path available in your collection that can be used as a cursor to scroll all of your documents. It should be unique and numerical.
8. Try *Test connection* and check the test passes
9. Select *Create*.
10. Create new *Analytics* project or select existing one.
11. In project page, select *Add to project -> Connection* .
12. Select *From platform* tab in *Add connection* page.
13. Select the name of your connection created in the previous step.
14. *Add to project -> Data Refinery Flow*
15. In Data Refinery flow page, select *Connection -> {name_of_connection} -> mySchema -> sample*.
16. You can preview the mocked data generated by this JDBC driver by clicking *Preview* button that has the mark of an eye.
17. Click *Add* button.
18. Save the Data Refinery flow.

We have some [scripts](/scripts) to make it easier to insert cursor to docments in your collection.