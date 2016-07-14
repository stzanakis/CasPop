package eu.europeana.tools;

import com.datastax.driver.core.*;
import com.datastax.driver.core.utils.UUIDs;
import eu.europeana.model.McsConstansts;
import eu.europeana.model.RevisionVocabulary;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2016-07-07
 */
public class CassandraPopulator {
    private static final Logger logger = LogManager.getLogger();

    public static void createProviderDataset(Session session, String provider, String dataset, String schema)
    {
        long startTime = System.currentTimeMillis();
        populateDataSets(session, provider, dataset, schema);
        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        logger.info("Populated (provider, dataset, schema) = (" + provider + ", " + dataset + ", " + schema + ")in: " + elapsedTime + "ms");
    }

    public static void fillInDataset(Session session, String provider, String dataset, String schema, int totalRecords, int batch)
    {
        ArrayList<String> cloudIds = new ArrayList<String>(batch);
        for(int i = 1; i <= totalRecords; i++) {
                cloudIds.add(Integer.toString(i));
        }

        long startTime = System.currentTimeMillis();
        populateRepresentations(session, provider, schema, cloudIds, RevisionVocabulary.UPLOAD.toString(), batch);
        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        logger.info("Populated total:" + totalRecords + " representations for provider: " + provider + " in: " + elapsedTime + "ms");

        startTime = System.currentTimeMillis();
        populateAssignments(session, provider, dataset, schema, cloudIds, RevisionVocabulary.UPLOAD.toString(), batch);
        stopTime = System.currentTimeMillis();
        elapsedTime = stopTime - startTime;
        logger.info("Populated total: " + totalRecords + " assignments for provider: " + provider + " and dataset: " + dataset + " in: " + elapsedTime + "ms");
    }

    public static void populateDataSets(Session session, String provider, String dataset, String schema) {
        Date date = new Date();
        //Data_sets
        session.execute(
                "INSERT INTO " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.DATA_SETS +
                        " (" + McsConstansts.PROVIDER_ID + ", " + McsConstansts.DATASET_ID + ", " + McsConstansts.CREATION_DATE + ", " + McsConstansts.UPDATED_TIMESTAMP + ", " + McsConstansts.DESCRIPTION + ") " +
                        "VALUES (?, ?, ?, ?, ?)",
                provider, dataset, new Timestamp(date.getTime()), new Timestamp(date.getTime()), dataset);

        //Data_sets_created
        session.execute(
                "INSERT INTO " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.DATA_SETS_CREATED +
                        " (" + McsConstansts.PROVIDER_ID + ", " + McsConstansts.DATASET_ID + ", " + McsConstansts.CREATION_DATE + ", " + McsConstansts.UPDATED_TIMESTAMP + ", " + McsConstansts.DESCRIPTION + ") " +
                        "VALUES (?, ?, ?, ?, ?)",
                provider, dataset, new Timestamp(date.getTime()), new Timestamp(date.getTime()), dataset);

        //Data_sets_updated
        session.execute(
                "INSERT INTO " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.DATA_SETS_UPDATED +
                        " (" + McsConstansts.PROVIDER_ID + ", " + McsConstansts.DATASET_ID + ", " + McsConstansts.CREATION_DATE + ", " + McsConstansts.UPDATED_TIMESTAMP + ", " + McsConstansts.DESCRIPTION + ") " +
                        "VALUES (?, ?, ?, ?, ?)",
                provider, dataset, new Timestamp(date.getTime()), new Timestamp(date.getTime()), dataset);

        //Dataset_schemas
        session.execute(
                "INSERT INTO " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.DATASET_SCHEMAS +
                        " (" + McsConstansts.PROVIDER_ID + ", " + McsConstansts.DATASET_ID + ", " + McsConstansts.SCHEMA_ID + ", " + McsConstansts.CREATION_DATE + ", " + McsConstansts.UPDATED_TIMESTAMP + ") " +
                        "VALUES (?, ?, ?)",
                provider, dataset, schema, new Timestamp(date.getTime()), new Timestamp(date.getTime()));
    }

    public static void insertDatasetSchema(Session session, String provider, String dataset, String schema) {
        Date date = new Date();
        session.execute("INSERT INTO " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.DATASET_SCHEMAS
                + " (" + McsConstansts.PROVIDER_ID + "," + McsConstansts.DATASET_ID + "," + McsConstansts.SCHEMA_ID + ", " + McsConstansts.CREATION_DATE + ", " + McsConstansts.UPDATED_TIMESTAMP + ") "
                + "VALUES (?, ?, ?, ?, ?)", provider, dataset, schema, new Timestamp(date.getTime()), new Timestamp(date.getTime()));

        //Dataset has been updated so update the datasets tables
        //Retrieve creation_date and updated_timestamp first
        ResultSet resultSet = session.execute("SELECT " + McsConstansts.CREATION_DATE + "," + McsConstansts.UPDATED_TIMESTAMP + " FROM " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.DATA_SETS
                + " WHERE " + McsConstansts.PROVIDER_ID + "=?" + " AND " + McsConstansts.DATASET_ID + "=?", provider, dataset);
        Row row = resultSet.iterator().next();
        Date creationDate = row.getTimestamp("creation_date");
        Date updated_timestamp = row.getTimestamp("updated_timestamp");

        session.execute("UPDATE " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.DATA_SETS + " SET "
                + McsConstansts.CREATION_DATE + "=?," + McsConstansts.UPDATED_TIMESTAMP + "=?" + " WHERE " + McsConstansts.PROVIDER_ID + "=?" + " AND " + McsConstansts.DATASET_ID + "=?"
                , new Timestamp(date.getTime()), new Timestamp(date.getTime()), provider, dataset);

        //Here we need to delete first because the date is part of the primary key
        session.execute("DELETE FROM " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.DATA_SETS_CREATED + " WHERE "
                + McsConstansts.PROVIDER_ID + "=?" + " AND " + McsConstansts.DATASET_ID + "=?" + " AND " + McsConstansts.CREATION_DATE + "=?"
                , provider, dataset, new Timestamp(creationDate.getTime()));
        session.execute(
                "INSERT INTO " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.DATA_SETS_CREATED +
                        " (" + McsConstansts.PROVIDER_ID + ", " + McsConstansts.DATASET_ID + ", " + McsConstansts.CREATION_DATE + ", " + McsConstansts.UPDATED_TIMESTAMP + ", " + McsConstansts.DESCRIPTION + ") " +
                        "VALUES (?, ?, ?, ?, ?)",
                provider, dataset, new Timestamp(creationDate.getTime()), new Timestamp(date.getTime()), dataset);

        session.execute("DELETE FROM " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.DATA_SETS_UPDATED + " WHERE "
                        + McsConstansts.PROVIDER_ID + "=?" + " AND " + McsConstansts.DATASET_ID + "=?" + " AND " + McsConstansts.UPDATED_TIMESTAMP + "=?"
                , provider, dataset, new Timestamp(updated_timestamp.getTime()));
        session.execute(
                "INSERT INTO " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.DATA_SETS_UPDATED +
                        " (" + McsConstansts.PROVIDER_ID + ", " + McsConstansts.DATASET_ID + ", " + McsConstansts.CREATION_DATE + ", " + McsConstansts.UPDATED_TIMESTAMP + ", " + McsConstansts.DESCRIPTION + ") " +
                        "VALUES (?, ?, ?, ?, ?)",
                provider, dataset, new Timestamp(creationDate.getTime()), new Timestamp(date.getTime()), dataset);
    }

    public static void populateAssignments(Session session, String  provider, String dataset, String schema, List<String> cloudIds, String revisionPrefix, int batch)
    {
        Date date = new Date();
        String revisionUpload = revisionPrefix + "-1";

        int totalRecords = cloudIds.size();
        for(int j = 1; j <= totalRecords; j+=batch) {
            long startTime = System.currentTimeMillis();
            PreparedStatement ps1 = session.prepare("INSERT INTO " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.DATA_SET_ASSIGNMENTS_CLOUD_ID +
                    " (" + McsConstansts.PROVIDER_ID + ", " + McsConstansts.DATASET_ID + ", " + McsConstansts.CLOUD_ID + ", " + McsConstansts.SCHEMA_ID + ", " + McsConstansts.REVISION_ID + ", " + McsConstansts.REVISION_TIMESTAMP + ", " + McsConstansts.ACCEPTANCE + ", " + McsConstansts.PUBLISHED + ", " + McsConstansts.MARK_DELETED + ") " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
            PreparedStatement ps2 = session.prepare("INSERT INTO " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.DATA_SET_ASSIGNMENTS_PROVIDER_DATASET_SCHEMA +
                    " (" + McsConstansts.PROVIDER_ID + ", " + McsConstansts.DATASET_ID + ", " + McsConstansts.CLOUD_ID + ", " + McsConstansts.SCHEMA_ID + ", " + McsConstansts.REVISION_ID + ", " + McsConstansts.REVISION_TIMESTAMP + ", " + McsConstansts.ACCEPTANCE + ", " + McsConstansts.PUBLISHED + ", " + McsConstansts.MARK_DELETED + ") " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
            PreparedStatement ps3 = session.prepare("INSERT INTO " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.DATA_SET_ASSIGNMENTS_PROVIDER_DATASET_REVISION +
                    " (" + McsConstansts.PROVIDER_ID + ", " + McsConstansts.DATASET_ID + ", " + McsConstansts.CLOUD_ID + ", " + McsConstansts.SCHEMA_ID + ", " + McsConstansts.REVISION_ID + ", " + McsConstansts.REVISION_TIMESTAMP + ", " + McsConstansts.ACCEPTANCE + ", " + McsConstansts.PUBLISHED + ", " + McsConstansts.MARK_DELETED + ") " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
            BatchStatement batch1 = new BatchStatement();
            BatchStatement batch2 = new BatchStatement();
            BatchStatement batch3 = new BatchStatement();
            for (int i = j; i < j + batch; i++) {

                String cloudId = cloudIds.get(i-1);

                //data_set_assignments_cloud_id
                batch1.add(ps1.bind(provider, dataset, cloudId, schema, revisionUpload, new Timestamp(date.getTime()), true, true, false));

                batch2.add(ps2.bind(provider, dataset, cloudId, schema, revisionUpload, new Timestamp(date.getTime()), true, true, false));

                batch3.add(ps3.bind(provider, dataset, cloudId, schema, revisionUpload, new Timestamp(date.getTime()), true, true, false));
            }
            session.execute(batch1);
            session.execute(batch2);
            session.execute(batch3);

            long stopTime = System.currentTimeMillis();
            long elapsedTime = stopTime - startTime;
            logger.info("Populated batch: " + batch + " assignments for provider: " + provider + " and dataset: " + dataset + " in: " + elapsedTime + "ms. Until now populated: " + (batch + j-1));
        }
    }

    public static void populateRepresentations(Session session, String provider, String schema, List<String> cloudIds, String revisionPrefix, int batch)
    {
        Date date = new Date();
        String revisionUpload = revisionPrefix + "-1";

        int totalRecords = cloudIds.size();
        for(int j = 1; j <= totalRecords; j+=batch) {
            long startTime = System.currentTimeMillis();
            PreparedStatement ps1 = session.prepare("INSERT INTO " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.REPRESENTATION_REVISIONS +
                    " (" + McsConstansts.CLOUD_ID + ", " + McsConstansts.SCHEMA_ID + ", " + McsConstansts.VERSION_ID + ", " + McsConstansts.REVISION_ID + ", " + McsConstansts.PROVIDER_ID + ", " + McsConstansts.PERSISTENT + ", " + McsConstansts.CREATION_DATE + ", " + McsConstansts.REVISION_TIMESTAMP + ") " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
            PreparedStatement ps2 = session.prepare("INSERT INTO " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.REPRESENTATION_REVISIONS_TIMESTAMP +
                    " (" + McsConstansts.CLOUD_ID + ", " + McsConstansts.SCHEMA_ID + ", " + McsConstansts.VERSION_ID + ", " + McsConstansts.REVISION_ID + ", " + McsConstansts.PROVIDER_ID + ", " + McsConstansts.PERSISTENT + ", " + McsConstansts.CREATION_DATE + ", " + McsConstansts.REVISION_TIMESTAMP + ") " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
            BatchStatement batch1 = new BatchStatement();
            BatchStatement batch2 = new BatchStatement();
            for (int i = j; i < j + batch; i++) {

                String cloudId = cloudIds.get(i-1);
                //representation_revisions
                batch1.add(ps1.bind(cloudId, schema, UUIDs.timeBased(), revisionUpload, provider, true, new Timestamp(date.getTime()), new Timestamp(date.getTime())));

                //representation_revision_timestamp
                batch2.add(ps2.bind(cloudId, schema, UUIDs.timeBased(), revisionUpload, provider, true, new Timestamp(date.getTime()), new Timestamp(date.getTime())));
            }
            session.execute(batch1);
            session.execute(batch2);

            long stopTime = System.currentTimeMillis();
            long elapsedTime = stopTime - startTime;
            logger.info("Populated batch: " + batch + " representations for provider " + provider + " in: " + elapsedTime + "ms. Until now populated: " + (batch + j-1));
        }

    }
}
