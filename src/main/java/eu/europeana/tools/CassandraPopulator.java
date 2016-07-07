package eu.europeana.tools;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import eu.europeana.model.DataSet;
import eu.europeana.model.McsConstansts;
import eu.europeana.model.Provider;
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

    public static void populateFirstProvider(Session session) {
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

        int batch = 100;
        ArrayList<String> cloudIds = new ArrayList<String>(batch);
        for(int i = 1; i <= batch; i++)
            cloudIds.add(Integer.toString(i));

        startTime = System.currentTimeMillis();
        populateRepresentations(session, provider, dataSet1, cloudIds);
        stopTime = System.currentTimeMillis();
        elapsedTime = stopTime - startTime;
        logger.info("Populated " + batch + " representations for provider" + provider.getProviderId() + " in: " + elapsedTime + "ms");

        startTime = System.currentTimeMillis();
        populateAssignments(session, provider, dataSet1, cloudIds);
        stopTime = System.currentTimeMillis();
        elapsedTime = stopTime - startTime;
        logger.info("Populated " + batch + " assignments for provider" + provider.getProviderId() + " in: " + elapsedTime + "ms");
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

    public static void populateAssignments(Session session, Provider provider, DataSet dataSet, List<String> cloudIds)
    {
        Date date = new Date();
        String revisionUpload = "UPLOAD-1";
        for (String cloudId :
                cloudIds) {
            //data_set_assignments_cloud_id
            String cql = "INSERT INTO " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.DATA_SET_ASSIGNMENTS_CLOUD_ID +
                    " (" + McsConstansts.PROVIDER_ID + ", " + McsConstansts.DATASET_ID + ", " + McsConstansts.CLOUD_ID + ", " + McsConstansts.SCHEMA_ID + ", " + McsConstansts.REVISION_ID + ", " + McsConstansts.REVISION_TIMESTAMP + ", " + McsConstansts.ACCEPTANCE + ", " + McsConstansts.PUBLISHED + ", " + McsConstansts.MARK_DELETED + ") " +
                    "VALUES ('" + provider.getProviderId() + "', '" + dataSet.getDatasetId() + "', '" + cloudId + "', '" + dataSet.getSchemas().iterator().next() + "', '" + revisionUpload + "', '" + new Timestamp(date.getTime()) + "', " + true + ", " + true + ", " + false + ")";
            session.execute(cql);

            cql = "INSERT INTO " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.DATA_SET_ASSIGNMENTS_PROVIDER_DATASET_SCHEMA +
                    " (" + McsConstansts.PROVIDER_ID + ", " + McsConstansts.DATASET_ID + ", " + McsConstansts.CLOUD_ID + ", " + McsConstansts.SCHEMA_ID + ", " + McsConstansts.REVISION_ID + ", " + McsConstansts.REVISION_TIMESTAMP + ", " + McsConstansts.ACCEPTANCE + ", " + McsConstansts.PUBLISHED + ", " + McsConstansts.MARK_DELETED + ") " +
                    "VALUES ('" + provider.getProviderId() + "', '" + dataSet.getDatasetId() + "', '" + cloudId + "', '" + dataSet.getSchemas().iterator().next() + "', '" + revisionUpload + "', '" + new Timestamp(date.getTime()) + "', " + true + ", " + true + ", " + false + ")";
            session.execute(cql);

            cql = "INSERT INTO " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.DATA_SET_ASSIGNMENTS_PROVIDER_DATASET_REVISION +
                    " (" + McsConstansts.PROVIDER_ID + ", " + McsConstansts.DATASET_ID + ", " + McsConstansts.CLOUD_ID + ", " + McsConstansts.SCHEMA_ID + ", " + McsConstansts.REVISION_ID + ", " + McsConstansts.REVISION_TIMESTAMP + ", " + McsConstansts.ACCEPTANCE + ", " + McsConstansts.PUBLISHED + ", " + McsConstansts.MARK_DELETED + ") " +
                    "VALUES ('" + provider.getProviderId() + "', '" + dataSet.getDatasetId() + "', '" + cloudId + "', '" + dataSet.getSchemas().iterator().next() + "', '" + revisionUpload + "', '" + new Timestamp(date.getTime()) + "', " + true + ", " + true + ", " + false + ")";
            session.execute(cql);
        }
    }

    public static void populateRepresentations(Session session, Provider provider, DataSet dataSet, List<String> cloudIds)
    {
        Date date = new Date();
        String revisionUpload = "UPLOAD-1";
        for (String cloudId :
                cloudIds) {
            //representation_revisions
            String cql = "INSERT INTO " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.REPRESENTATION_REVISIONS +
                    " (" + McsConstansts.CLOUD_ID+ ", " + McsConstansts.SCHEMA_ID + ", " + McsConstansts.VERSION_ID + ", " + McsConstansts.REVISION_ID + ", " + McsConstansts.PROVIDER_ID + ", " + McsConstansts.PERSISTENT + ", " + McsConstansts.CREATION_DATE + ", " + McsConstansts.REVISION_TIMESTAMP + ") " +
                    "VALUES ('" + cloudId + "', '" + dataSet.getSchemas().iterator().next() + "', " + UUIDs.timeBased() + ", '" + revisionUpload + "', '" + provider.getProviderId() + "', " + true + ", '" + new Timestamp(date.getTime()) + "', '" + new Timestamp(date.getTime()) + "')";
            session.execute(cql);

            //representation_revision_timestamp
            cql = "INSERT INTO " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.REPRESENTATION_REVISIONS_TIMESTAMP +
                    " (" + McsConstansts.CLOUD_ID+ ", " + McsConstansts.SCHEMA_ID + ", " + McsConstansts.VERSION_ID + ", " + McsConstansts.REVISION_ID + ", " + McsConstansts.PROVIDER_ID + ", " + McsConstansts.PERSISTENT + ", " + McsConstansts.CREATION_DATE + ", " + McsConstansts.REVISION_TIMESTAMP + ") " +
                    "VALUES ('" + cloudId + "', '" + dataSet.getSchemas().iterator().next() + "', " + UUIDs.timeBased() + ", '" + revisionUpload + "', '" + provider.getProviderId() + "', " + true + ", '" + new Timestamp(date.getTime()) + "', '" + new Timestamp(date.getTime()) + "')";
            session.execute(cql);
        }

    }
}
