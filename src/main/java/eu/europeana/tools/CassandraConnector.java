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
public class CassandraConnector implements AutoCloseable {
    private static CassandraConnector instance = null;
    private Session session;
    private Cluster cluster;

    private CassandraConnector() throws FileNotFoundException, ConfigurationException {
        String configurationDirectory = "/data/credentials/caspop";
        String configurationFileName = "caspop.properties";
        PropertiesConfiguration propertiesConfiguration = Configuration.loadConfiguration(configurationDirectory, configurationFileName);

        String[] ips = propertiesConfiguration.getStringArray("IPS");
        String port = propertiesConfiguration.getProperty("PORT").toString();

        ArrayList<InetSocketAddress> inetSocketAddresses = new ArrayList<InetSocketAddress>();
        inetSocketAddresses.add(new InetSocketAddress(ips[0], Integer.parseInt(port)));
        inetSocketAddresses.add(new InetSocketAddress(ips[1], Integer.parseInt(port)));
        inetSocketAddresses.add(new InetSocketAddress(ips[2], Integer.parseInt(port)));
        cluster = Cluster.builder().addContactPointsWithPorts(inetSocketAddresses).build();
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

    @Override
    public void close() throws Exception {
        session.close();
        cluster.close();
    }
}
