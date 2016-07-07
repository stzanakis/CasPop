package eu.europeana.tools;

import eu.europeana.model.DataSet;
import eu.europeana.model.DefaultPrefixes;
import eu.europeana.model.Provider;

import java.util.HashSet;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2016-07-07
 */
public class RandomEntitiesGenerator {

    private static int providerCounter = 1;
    private static int datasetCounter = 1;

    public static Provider generateProvider()
    {
        return new Provider(DefaultPrefixes.PROVIDER.getPrefix() + providerCounter++);
    }

    public static DataSet generateDataSet()
    {
        HashSet<String> schemas = new HashSet<String>();
        schemas.add("Schema1");
        return new DataSet(DefaultPrefixes.DATASET.getPrefix() + datasetCounter++, "description", schemas);
    }
}
