package gov.epa.emissions.commons.db;

import gov.epa.emissions.commons.io.importer.PersistenceTestCase;

import org.hibernate.Session;
import org.hibernate.SessionFactory;

public abstract class HibernateTestCase extends PersistenceTestCase {

    protected Session session;

    private SessionFactory sessionFactory;

    protected void setUp() throws Exception {
        super.setUp();
        sessionFactory = sessionFactory();
        session = sessionFactory.openSession();
    }

    protected void doTearDown() throws Exception {
        session.close();
    }

    private SessionFactory sessionFactory() throws Exception {
        LocalHibernateConfiguration config = new LocalHibernateConfiguration();
        return config.factory();
    }
}
