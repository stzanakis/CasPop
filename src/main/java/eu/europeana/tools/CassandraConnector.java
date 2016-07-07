package eu.europeana.tools;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.io.FileNotFoundException;
import java.net.InetSocketAddress;
import java.util.ArrayList;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2016-07-07
 */
public class CassandraConnector {
    private static CassandraConnector instance = null;
    private Session session;
    private Cluster cluster;

    private CassandraConnector() throws FileNotFoundException, ConfigurationException {
        String configurationDirectory = "/data/credentials/caspop";
        String configurationFileName = "caspop.properties";
        PropertiesConfiguration propertiesConfiguration = Configuration.loadConfiguration(configurationDirectory, configurationFileName);


        String ip1 = propertiesConfiguration.getProperty("IP1").toString();
        String port1 = propertiesConfiguration.getProperty("PORT1").toString();

        new ArrayList<InetSocketAddress>();
        cluster = Cluster.builder().addContactPointsWithPorts(new InetSocketAddress(ip1, Integer.parseInt(port1))).build();
    }

    public static CassandraConnector getInstance() throws FileNotFoundException, ConfigurationException {
        if (instance == null) {
            instance = new CassandraConnector();
        }
        return instance;
    }

    public Session getSession() {
        session = cluster.connect();
        return session;
    }

    public void closeSession() {
        session.close();
    }

    public void closeConnection() {
        cluster.close();
    }
}
