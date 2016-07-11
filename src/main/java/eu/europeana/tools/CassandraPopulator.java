package eu.europeana.tools;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import eu.europeana.model.DataSet;
import eu.europeana.model.McsConstansts;
import eu.europeana.model.Provider;
import eu.europeana.model.RevisionVocabulary;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2016-07-07
 */
public class CassandraPopulator {
    private static final Logger logger = LogManager.getLogger();

    public static void populateFirstProvider(Session session, int totalRecords, int batch) {
        Provider provider = RandomEntitiesGenerator.generateProvider();
        DataSet dataSet1 = RandomEntitiesGenerator.generateDataSet();
        DataSet dataSet2 = RandomEntitiesGenerator.generateDataSet();
        HashSet<DataSet> dataSets = new HashSet<DataSet>();
        dataSets.add(dataSet1);
        dataSets.add(dataSet2);
        provider.setDatasets(dataSets);

        long startTime = System.currentTimeMillis();
        populateDataSets(session, provider, dataSet1);
        populateDataSets(session, provider, dataSet2);
        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        logger.info("Populated first providers datasets in: " + elapsedTime + "ms");

        ArrayList<String> cloudIds = new ArrayList<String>(batch);
        for(int j = 1; j <= totalRecords; j+=batch) {
            for (int i = j; i < j + batch; i++) {
                cloudIds.add(Integer.toString(i));
            }
        }

        startTime = System.currentTimeMillis();
        populateRepresentations(session, provider, dataSet1, cloudIds, RevisionVocabulary.UPLOAD.toString(), batch);
        stopTime = System.currentTimeMillis();
        elapsedTime = stopTime - startTime;
        logger.info("Populated total:" + totalRecords + " representations for provider " + provider.getProviderId() + " in: " + elapsedTime + "ms");

        startTime = System.currentTimeMillis();
        populateAssignments(session, provider, dataSet1, cloudIds, RevisionVocabulary.UPLOAD.toString(), batch);
        stopTime = System.currentTimeMillis();
        elapsedTime = stopTime - startTime;
        logger.info("Populated total: " + totalRecords + " assignments for provider " + provider.getProviderId() + " in: " + elapsedTime + "ms");
    }

    public static void populateDataSets(Session session, Provider provider, DataSet dataSet) {
        Date date = new Date();
        //Data_sets
        String cql = "INSERT INTO " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.DATA_SETS +
                " (" + McsConstansts.PROVIDER_ID + ", " + McsConstansts.DATASET_ID + ", " + McsConstansts.CREATION_DATE + ", " + McsConstansts.UPDATED_TIMESTAMP + ", " + McsConstansts.DESCRIPTION + ") " +
                "VALUES ('" + provider.getProviderId() + "', '" + dataSet.getDatasetId() + "', '" + new Timestamp(date.getTime()) + "', '" + new Timestamp(date.getTime()) + "', '" + dataSet.getDescription() + "')";
        session.execute(cql);

        //Data_sets_created
        cql = "INSERT INTO " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.DATA_SETS_CREATED +
                " (" + McsConstansts.PROVIDER_ID + ", " + McsConstansts.DATASET_ID + ", " + McsConstansts.CREATION_DATE + ", " + McsConstansts.UPDATED_TIMESTAMP + ", " + McsConstansts.DESCRIPTION + ") " +
                "VALUES ('" + provider.getProviderId() + "', '" + dataSet.getDatasetId() + "', '" + new Timestamp(date.getTime()) + "', '" + new Timestamp(date.getTime()) + "', '" + dataSet.getDescription() + "')";
        session.execute(cql);

        //Data_sets_updated
        cql = "INSERT INTO " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.DATA_SETS_UPDATED +
                " (" + McsConstansts.PROVIDER_ID + ", " + McsConstansts.DATASET_ID + ", " + McsConstansts.CREATION_DATE + ", " + McsConstansts.UPDATED_TIMESTAMP + ", " + McsConstansts.DESCRIPTION + ") " +
                "VALUES ('" + provider.getProviderId() + "', '" + dataSet.getDatasetId() + "', '" + new Timestamp(date.getTime()) + "', '" + new Timestamp(date.getTime()) + "', '" + dataSet.getDescription() + "')";
        session.execute(cql);

        //Dataset_schemas
        cql = "INSERT INTO " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.DATASET_SCHEMAS +
                " (" + McsConstansts.PROVIDER_ID + ", " + McsConstansts.DATASET_ID + ", " + McsConstansts.SCHEMA_ID + ") " +
                "VALUES ('" + provider.getProviderId() + "', '" + dataSet.getDatasetId() + "', '" + dataSet.getSchemas().iterator().next() + "')";
        session.execute(cql);
    }

    public static void populateAssignments(Session session, Provider provider, DataSet dataSet, List<String> cloudIds, String revisionPrefix, int batch)
    {
        Date date = new Date();
        String revisionUpload = revisionPrefix + "-1";

        int totalRecords = cloudIds.size();
        for(int j = 1; j <= totalRecords; j+=batch) {
            long startTime = System.currentTimeMillis();
            StringBuilder stringBuilder1 = new StringBuilder("BEGIN BATCH ");
            StringBuilder stringBuilder2 = new StringBuilder("BEGIN BATCH ");
            StringBuilder stringBuilder3 = new StringBuilder("BEGIN BATCH ");
            for (int i = j; i < j + batch; i++) {

                String cloudId = cloudIds.get(i-1);

                //data_set_assignments_cloud_id
                stringBuilder1.append("INSERT INTO " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.DATA_SET_ASSIGNMENTS_CLOUD_ID +
                        " (" + McsConstansts.PROVIDER_ID + ", " + McsConstansts.DATASET_ID + ", " + McsConstansts.CLOUD_ID + ", " + McsConstansts.SCHEMA_ID + ", " + McsConstansts.REVISION_ID + ", " + McsConstansts.REVISION_TIMESTAMP + ", " + McsConstansts.ACCEPTANCE + ", " + McsConstansts.PUBLISHED + ", " + McsConstansts.MARK_DELETED + ") " +
                        "VALUES ('" + provider.getProviderId() + "', '" + dataSet.getDatasetId() + "', '" + cloudId + "', '" + dataSet.getSchemas().iterator().next() + "', '" + revisionUpload + "', '" + new Timestamp(date.getTime()) + "', " + true + ", " + true + ", " + false + ");");

                stringBuilder2.append("INSERT INTO " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.DATA_SET_ASSIGNMENTS_PROVIDER_DATASET_SCHEMA +
                        " (" + McsConstansts.PROVIDER_ID + ", " + McsConstansts.DATASET_ID + ", " + McsConstansts.CLOUD_ID + ", " + McsConstansts.SCHEMA_ID + ", " + McsConstansts.REVISION_ID + ", " + McsConstansts.REVISION_TIMESTAMP + ", " + McsConstansts.ACCEPTANCE + ", " + McsConstansts.PUBLISHED + ", " + McsConstansts.MARK_DELETED + ") " +
                        "VALUES ('" + provider.getProviderId() + "', '" + dataSet.getDatasetId() + "', '" + cloudId + "', '" + dataSet.getSchemas().iterator().next() + "', '" + revisionUpload + "', '" + new Timestamp(date.getTime()) + "', " + true + ", " + true + ", " + false + ");");

                stringBuilder3.append("INSERT INTO " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.DATA_SET_ASSIGNMENTS_PROVIDER_DATASET_REVISION +
                        " (" + McsConstansts.PROVIDER_ID + ", " + McsConstansts.DATASET_ID + ", " + McsConstansts.CLOUD_ID + ", " + McsConstansts.SCHEMA_ID + ", " + McsConstansts.REVISION_ID + ", " + McsConstansts.REVISION_TIMESTAMP + ", " + McsConstansts.ACCEPTANCE + ", " + McsConstansts.PUBLISHED + ", " + McsConstansts.MARK_DELETED + ") " +
                        "VALUES ('" + provider.getProviderId() + "', '" + dataSet.getDatasetId() + "', '" + cloudId + "', '" + dataSet.getSchemas().iterator().next() + "', '" + revisionUpload + "', '" + new Timestamp(date.getTime()) + "', " + true + ", " + true + ", " + false + ");");
            }
            stringBuilder1.append(" APPLY BATCH;");
            stringBuilder2.append(" APPLY BATCH;");
            stringBuilder3.append(" APPLY BATCH;");
            session.execute(stringBuilder1.toString());
            session.execute(stringBuilder2.toString());
            session.execute(stringBuilder3.toString());

            long stopTime = System.currentTimeMillis();
            long elapsedTime = stopTime - startTime;
            logger.info("Populated batch: " + batch + " assignments for provider " + provider.getProviderId() + " in: " + elapsedTime + "ms. Until now populated: " + (batch + j-1));
        }
    }

    public static void populateRepresentations(Session session, Provider provider, DataSet dataSet, List<String> cloudIds, String revisionPrefix, int batch)
    {
        Date date = new Date();
        String revisionUpload = revisionPrefix + "-1";

        int totalRecords = cloudIds.size();
        for(int j = 1; j <= totalRecords; j+=batch) {
            long startTime = System.currentTimeMillis();
            StringBuilder stringBuilder1 = new StringBuilder("BEGIN BATCH ");
            StringBuilder stringBuilder2 = new StringBuilder("BEGIN BATCH ");
            for (int i = j; i < j + batch; i++) {

                String cloudId = cloudIds.get(i-1);
                //representation_revisions
                stringBuilder1.append("INSERT INTO " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.REPRESENTATION_REVISIONS +
                        " (" + McsConstansts.CLOUD_ID + ", " + McsConstansts.SCHEMA_ID + ", " + McsConstansts.VERSION_ID + ", " + McsConstansts.REVISION_ID + ", " + McsConstansts.PROVIDER_ID + ", " + McsConstansts.PERSISTENT + ", " + McsConstansts.CREATION_DATE + ", " + McsConstansts.REVISION_TIMESTAMP + ") " +
                        "VALUES ('" + cloudId + "', '" + dataSet.getSchemas().iterator().next() + "', " + UUIDs.timeBased() + ", '" + revisionUpload + "', '" + provider.getProviderId() + "', " + true + ", '" + new Timestamp(date.getTime()) + "', '" + new Timestamp(date.getTime()) + "');");

                //representation_revision_timestamp
                stringBuilder2.append("INSERT INTO " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.REPRESENTATION_REVISIONS_TIMESTAMP +
                        " (" + McsConstansts.CLOUD_ID + ", " + McsConstansts.SCHEMA_ID + ", " + McsConstansts.VERSION_ID + ", " + McsConstansts.REVISION_ID + ", " + McsConstansts.PROVIDER_ID + ", " + McsConstansts.PERSISTENT + ", " + McsConstansts.CREATION_DATE + ", " + McsConstansts.REVISION_TIMESTAMP + ") " +
                        "VALUES ('" + cloudId + "', '" + dataSet.getSchemas().iterator().next() + "', " + UUIDs.timeBased() + ", '" + revisionUpload + "', '" + provider.getProviderId() + "', " + true + ", '" + new Timestamp(date.getTime()) + "', '" + new Timestamp(date.getTime()) + "');");
            }
            stringBuilder1.append(" APPLY BATCH;");
            stringBuilder2.append(" APPLY BATCH;");
            session.execute(stringBuilder1.toString());
            session.execute(stringBuilder2.toString());

            long stopTime = System.currentTimeMillis();
            long elapsedTime = stopTime - startTime;
            logger.info("Populated batch: " + batch + " representations for provider " + provider.getProviderId() + " in: " + elapsedTime + "ms. Until now populated: " + (batch + j-1));
        }

    }
}
