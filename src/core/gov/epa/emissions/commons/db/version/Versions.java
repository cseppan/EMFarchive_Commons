package gov.epa.emissions.commons.db.version;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.collections.primitives.ArrayIntList;
import org.apache.commons.collections.primitives.IntList;
import org.hibernate.Criteria;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Restrictions;

public class Versions {

    public Version[] getPath(long datasetId, int finalVersion, Session session) {
        Version version = get(datasetId, finalVersion, session);
        if (version == null)
            return new Version[0];
        return doGetPath(version, session);
    }

    private Version[] doGetPath(Version version, Session session) {
        int[] parentVersions = parseParentVersions(version.getPath());
        List versions = new ArrayList();
        for (int i = 0; i < parentVersions.length; i++) {
            Version parent = get(version.getDatasetId(), parentVersions[i], session);
            versions.add(parent);
        }

        versions.add(version);

        return (Version[]) versions.toArray(new Version[0]);
    }

    public Version get(long datasetId, int version, Session session) {
        Transaction tx = null;
        try {
            tx = session.beginTransaction();
            Criteria crit = session.createCriteria(Version.class);
            Criteria fullCrit = crit.add(Restrictions.eq("datasetId", new Long(datasetId))).add(
                    Restrictions.eq("version", new Integer(version)));
            tx.commit();

            return (Version) fullCrit.uniqueResult();
        } catch (HibernateException e) {
            tx.rollback();
            throw e;
        }
    }

    private int[] parseParentVersions(String versionsList) {
        IntList versions = new ArrayIntList();

        StringTokenizer tokenizer = new StringTokenizer(versionsList, ",");
        while (tokenizer.hasMoreTokens()) {
            int token = Integer.parseInt(tokenizer.nextToken());
            versions.add(token);
        }

        return versions.toArray();
    }

    public int getLastFinalVersion(long datasetId, Session session) {
        int versionNumber = 0;

        Version[] versions = get(datasetId, session);

        for (int i = 0; i < versions.length; i++) {
            int versNum = versions[i].getVersion();

            if (versNum > versionNumber) {
                versionNumber = versions[i].getVersion();
            }
        }

        return versionNumber;
    }

    public Version[] get(long datasetId, Session session) {
        Transaction tx = null;
        try {
            tx = session.beginTransaction();
            Criteria crit = session.createCriteria(Version.class)
                    .add(Restrictions.eq("datasetId", new Long(datasetId)));
            List versions = crit.list();
            tx.commit();

            return (Version[]) versions.toArray(new Version[0]);
        } catch (HibernateException e) {
            tx.rollback();
            throw e;
        }
    }

    public Version derive(Version base, String name, Session session) {
        if (!base.isFinalVersion())
            throw new RuntimeException("cannot derive a new version from a non-final version");

        Version version = new Version();
        int newVersionNum = getNextVersionNumber(base.getDatasetId(), session);

        version.setName(name);
        version.setVersion(newVersionNum);
        version.setPath(path(base));
        version.setDatasetId(base.getDatasetId());
        version.setDate(new Date());
        // version.setCreator(user); TODO

        save(version, session);

        return version;
    }

    private void save(Version version, Session session) {
        Transaction tx = null;
        try {
            tx = session.beginTransaction();
            session.save(version);
            tx.commit();
        } catch (HibernateException e) {
            tx.rollback();
            throw e;
        }
    }

    public Version markFinal(Version derived, Session session) {
        derived.markFinal();
        derived.setDate(new Date());

        Transaction tx = null;
        try {
            tx = session.beginTransaction();
            session.update(derived);
            tx.commit();
        } catch (HibernateException e) {
            tx.rollback();
            throw e;
        }

        return derived;
    }

    private String path(Version base) {
        return base.createPathForDerived();
    }

    private int getNextVersionNumber(long datasetId, Session session) {
        Transaction tx = null;
        try {
            tx = session.beginTransaction();
            Criteria base = session.createCriteria(Version.class);
            Criteria fullCrit = base.add(Restrictions.eq("datasetId", new Long(datasetId))).addOrder(
                    Order.desc("version"));
            List versions = fullCrit.list();
            tx.commit();

            Version latest = (Version) versions.get(0);
            return (latest).getVersion() + 1;
        } catch (HibernateException e) {
            tx.rollback();
            throw e;
        }
    }

    public Version current(Version version, Session session) {
        return get(version.getDatasetId(), version.getVersion(), session);
    }

}
