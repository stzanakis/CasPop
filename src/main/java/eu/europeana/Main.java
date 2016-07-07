package eu.europeana;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import eu.europeana.model.McsConstansts;
import eu.europeana.tools.CassandraConnector;
import eu.europeana.tools.CassandraPopulator;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileNotFoundException;
import java.util.List;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2016-07-07
 */
public class Main {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String args[]) throws FileNotFoundException, ConfigurationException {
        System.out.println("How you doin?");

        CassandraConnector cassandraConnector = CassandraConnector.getInstance();
        Session session = cassandraConnector.getSession();

        CassandraPopulator.populateFirstProvider(session);

        ResultSet execute = session.execute("SELECT * FROM " + McsConstansts.KEYSPACEMCS + "." + McsConstansts.DATA_SETS);
        List<Row> all = execute.all();
        for (Row row :
                all) {
            System.out.println(row);
        }

        cassandraConnector.closeSession();
        cassandraConnector.closeConnection();

    }
}
