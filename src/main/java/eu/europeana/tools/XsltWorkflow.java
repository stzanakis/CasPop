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
 * @since 2016-07-14
 */
public class XsltWorkflow {
    private static final Logger logger = LogManager.getLogger();
    public static void transformRecordsFromRevision(Session session, String provider, String dataset, String schema, String revision, String newSchema, int fetchSize, int rowsThreshold, int batch)
    {
        List<String> cloudIds = new ArrayList<String>(fetchSize);
        Statement stmt = new SimpleStatement("SELECT cloud_id FROM " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.DATA_SET_ASSIGNMENTS_PROVIDER_DATASET_REVISION
                + " WHERE " + McsConstansts.PROVIDER_ID + "=?" + " AND " + McsConstansts.DATASET_ID + "=?" + " AND " + McsConstansts.SCHEMA_ID + "=?"
                + " AND " + McsConstansts.REVISION_ID + "=?", provider, dataset, schema, revision);
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
                List<ResultSet> resultSets = retrieveRepresentations(session, cloudIds, schema, revision);
                //Process file and send to swift and now store again in cassandra
                createNewSchemaRepresentation(session, cloudIds, provider, dataset, newSchema, batch);
                logger.info("Total processed until now: " + counter);
                cloudIds = new ArrayList<String>(fetchSize);
            }
        }
        if(counter%fetchSize != 0) {
            List<ResultSet> resultSets = retrieveRepresentations(session, cloudIds, schema, revision);
            //Process file and send to swift and now store again in cassandra
            createNewSchemaRepresentation(session, cloudIds, provider, dataset, newSchema, batch);
        }
        logger.info("Total processed: " + counter);

    }

    public static List<ResultSet> retrieveRepresentations(Session session, List<String> cloudIds, String schema, String revision)
    {
        ArrayList<ResultSet> resultSets = new ArrayList<>();
        for (String cloudId : cloudIds) {
            ResultSet resultSet = session.execute("SELECT * FROM " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.REPRESENTATION_REVISIONS
                    + " WHERE " + McsConstansts.CLOUD_ID + "=?" + " AND " + McsConstansts.SCHEMA_ID + "=?" + " AND " + McsConstansts.REVISION_ID + "=?"
                    , cloudId, schema, revision);
            resultSets.add(resultSet);
        }
        return resultSets;
    }

    public static void createNewSchemaRepresentation(Session session, List<String> cloudIds, String provider, String dataset, String newSchema, int batch)
    {
        long startTime = System.currentTimeMillis();
        CassandraPopulator.populateRepresentations(session, provider, newSchema, cloudIds, RevisionVocabulary.TRANSFORM.toString(), batch);
        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        logger.info("Populated total:" + cloudIds.size() + " representations for provider " + provider + " in: " + elapsedTime + "ms");

        startTime = System.currentTimeMillis();
        CassandraPopulator.populateAssignments(session, provider, dataset, newSchema, cloudIds, RevisionVocabulary.TRANSFORM.toString(), batch);
        stopTime = System.currentTimeMillis();
        elapsedTime = stopTime - startTime;
        logger.info("Populated total: " + cloudIds.size() + " assignments for provider: " + provider + " and dataset: " + dataset + " in: " + elapsedTime + "ms");
    }
}
