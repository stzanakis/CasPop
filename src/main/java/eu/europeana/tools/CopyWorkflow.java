package eu.europeana.tools;

import com.datastax.driver.core.*;
import eu.europeana.model.DataSet;
import eu.europeana.model.McsConstansts;
import eu.europeana.model.Provider;
import eu.europeana.model.RevisionVocabulary;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2016-07-11
 */
public class CopyWorkflow {
    private static final Logger logger = LogManager.getLogger();
    public static void copyFromProviderDatasetPublished(Session session, String providerFrom, String providerTo, String datasetFrom, String datasetTo, String schema, int fetchSize, int rowsThreshold, int limit)
    {
        String query = "SELECT * FROM " + McsConstansts.KEYSPACEMCS + "." + "data_set_assignments_provider_dataset_schema WHERE " +
                McsConstansts.PROVIDER_ID + "='" + providerFrom + "' AND " + McsConstansts.DATASET_ID + "='" + datasetFrom + "' AND " + McsConstansts.SCHEMA_ID + "='"
                + schema + "' AND " + McsConstansts.REVISION_TIMESTAMP + ">='1970-01-01 00:00:01" + "' AND "
                + McsConstansts.PUBLISHED + "=true" + (limit>0?" LIMIT " + limit:"");

        List<String> cloudIds = new ArrayList<String>(fetchSize);
        Statement stmt = new SimpleStatement(query);
        stmt.setFetchSize(fetchSize);
        ResultSet rs = session.execute(stmt);
        Iterator<Row> iter = rs.iterator();
        int counter = 0;
        while (iter.hasNext()) {
            if (rs.getAvailableWithoutFetching() == rowsThreshold && !rs.isFullyFetched())
                rs.fetchMoreResults();

            Row row = iter.next();
            cloudIds.add(row.getString("cloud_id"));
            counter++;

            if(counter%fetchSize == 0) {
                cloudIds = new ArrayList<String>(fetchSize);
//                copyFromProviderDatasetPublished(session, providerTo, datasetTo, schema, cloudIds);
//                System.out.println(counter);
            }
        }
//        if(counter%fetchSize != 0)
//            copyFromProviderDatasetPublished(session, providerTo, datasetTo, schema, cloudIds);
//        System.out.println(counter);


    }

    private static void copyFromProviderDatasetPublished(Session session, String providerTo, String datasetTo, String schema, List<String> cloudIds)
    {
        //populate the dataset
        Provider provider = new Provider(providerTo);
        HashSet<String> schemas = new HashSet<String>();
        schemas.add(schema);
        DataSet dataSet = new DataSet(datasetTo, "Test", schemas);
        HashSet<DataSet> dataSets = new HashSet<DataSet>();
        dataSets.add(dataSet);
        provider.setDatasets(dataSets);

        long startTime = System.currentTimeMillis();
        CassandraPopulator.populateDataSets(session, provider, dataSet);
        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        logger.info("Populated " + providerTo + " and " + datasetTo + " in: " + elapsedTime + "ms");

        int batch = 100;
        startTime = System.currentTimeMillis();
        CassandraPopulator.populateRepresentations(session, provider, dataSet, cloudIds, RevisionVocabulary.COPY.toString(), batch);
        stopTime = System.currentTimeMillis();
        elapsedTime = stopTime - startTime;
        logger.info("Populated total:" + cloudIds.size() + " representations for provider " + provider.getProviderId() + " in: " + elapsedTime + "ms");

        startTime = System.currentTimeMillis();
        CassandraPopulator.populateAssignments(session, provider, dataSet, cloudIds, RevisionVocabulary.COPY.toString(), batch);
        stopTime = System.currentTimeMillis();
        elapsedTime = stopTime - startTime;
        logger.info("Populated total: " + cloudIds.size() + " assignments for provider " + provider.getProviderId() + " in: " + elapsedTime + "ms");
    }

}
