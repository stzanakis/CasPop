package eu.europeana.tools;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import eu.europeana.model.DataSet;
import eu.europeana.model.McsConstansts;
import eu.europeana.model.Provider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.List;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2016-07-11
 */
public class CopyWorkflow {
    private static final Logger logger = LogManager.getLogger();
    public static void copyFromProviderDatasetPublished(Session session, String providerFrom, String providerTo, String datasetFrom, String datasetTo, String schema)
    {
        int limit=10000;

        String query = "SELECT * FROM " + McsConstansts.KEYSPACEMCS + "." + "data_set_assignments_provider_dataset_schema WHERE " +
                McsConstansts.PROVIDER_ID + "='" + providerFrom + "' AND " + McsConstansts.DATASET_ID + "='" + datasetFrom + "' AND " + McsConstansts.SCHEMA_ID + "='"
                + schema + "' AND " + McsConstansts.REVISION_TIMESTAMP + ">'1970-01-01 00:00:01" + "' AND " + McsConstansts.PUBLISHED + "=true" + " LIMIT " + limit ;

        ResultSet execute = session.execute(query);
        List<Row> all = execute.all();

        for (Row row :
                all) {
            System.out.println(row);
        }
        System.out.println(all.size());

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

    }

}
