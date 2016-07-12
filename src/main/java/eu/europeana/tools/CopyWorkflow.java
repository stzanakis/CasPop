package eu.europeana.tools;

import com.datastax.driver.core.*;
import eu.europeana.model.McsConstansts;
import eu.europeana.model.RevisionVocabulary;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2016-07-11
 */
public class CopyWorkflow {
    private static final Logger logger = LogManager.getLogger();
    public static void copyFromProviderDatasetPublished(Session session, String providerFrom, String providerTo, String datasetFrom, String datasetTo, String schema, int fetchSize, int rowsThreshold, int limit, int batch)
    {
        String query = "SELECT * FROM " + McsConstansts.KEYSPACEMCS + "." + "data_set_assignments_provider_dataset_schema WHERE " +
                McsConstansts.PROVIDER_ID + "='" + providerFrom + "' AND " + McsConstansts.DATASET_ID + "='" + datasetFrom + "' AND " + McsConstansts.SCHEMA_ID + "='"
                + schema + "' AND " + McsConstansts.REVISION_TIMESTAMP + ">='1970-01-01 00:00:01" + "' AND "
                + McsConstansts.PUBLISHED + "=true" + (limit>0?" LIMIT " + limit:"");

        List<String> cloudIds = new ArrayList<String>(fetchSize);
        Statement stmt = new SimpleStatement(query);
        stmt.setFetchSize(fetchSize);
        ResultSet rs = session.execute(stmt);
        Iterator<Row> iterator = rs.iterator();
        int counter = 0;
        while (iterator.hasNext()) {
            if (rs.getAvailableWithoutFetching() == rowsThreshold && !rs.isFullyFetched())
                rs.fetchMoreResults();

            Row row = iterator.next();
            cloudIds.add(row.getString("cloud_id"));
            counter++;

            if(counter%fetchSize == 0) {
                populateToProviderDatasetPublished(session, providerTo, datasetTo, schema, cloudIds, batch);
                logger.info("Total processed until now: " + counter);
                cloudIds = new ArrayList<String>(fetchSize);
            }
        }
        if(counter%fetchSize != 0)
            populateToProviderDatasetPublished(session, providerTo, datasetTo, schema, cloudIds, batch);
        logger.info("Total processed: " + counter);


    }

    private static void populateToProviderDatasetPublished(Session session, String provider, String dataset, String schema, List<String> cloudIds, int batch)
    {
        long startTime = System.currentTimeMillis();
        CassandraPopulator.populateRepresentations(session, provider, dataset, cloudIds, RevisionVocabulary.COPY.toString(), batch);
        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        logger.info("Populated total:" + cloudIds.size() + " representations for provider " + provider + " in: " + elapsedTime + "ms");

        startTime = System.currentTimeMillis();
        CassandraPopulator.populateAssignments(session, provider, dataset, schema, cloudIds, RevisionVocabulary.COPY.toString(), batch);
        stopTime = System.currentTimeMillis();
        elapsedTime = stopTime - startTime;
        logger.info("Populated total: " + cloudIds.size() + " assignments for provider: " + provider + " and dataset: " + dataset + " in: " + elapsedTime + "ms");
    }

}
