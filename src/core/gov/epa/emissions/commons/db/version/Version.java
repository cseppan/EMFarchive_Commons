package gov.epa.emissions.commons.db.version;

import gov.epa.emissions.commons.io.Lockable;
import gov.epa.emissions.commons.io.Mutex;
import gov.epa.emissions.commons.security.User;

import java.util.Date;

public class Version implements Lockable {

    private long id;

    private long datasetId;

    private int version;

    private String path;

    private boolean finalVersion = false;

    private String name;

    private Date date;

    private User creator;

    private Mutex lock;

    public Version() {
        lock = new Mutex();
    }

    public boolean isFinalVersion() {
        return finalVersion;
    }

    public void markFinal() {
        this.finalVersion = true;
    }

    public int getVersion() {
        return version;
    }

    public void setDatasetId(long datasetId) {
        this.datasetId = datasetId;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public long getDatasetId() {
        return datasetId;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public void setFinalVersion(boolean finalVersion) {
        this.finalVersion = finalVersion;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    /**
     * create path for a versioned derived from 'me' i.e. I am it's base.
     */
    public String createPathForDerived() {
        return path.length() == 0 ? (version + "") : (path + "," + version);
    }

    public long getBase() {
        if (version == 0)// i.e. root
            return 0;

        int start = path.lastIndexOf(",") + 1;
        return Long.parseLong(path.substring(start));
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public Date getDate() {
        return date;
    }

    public void setCreator(User creator) {
        this.creator = creator;
    }

    public User getCreator() {
        return creator;
    }

    public String toString() {
        return "Version " + version + " of Dataset - " + datasetId;
    }

    public Date getLockDate() {
        return lock.getLockDate();
    }

    public void setLockDate(Date lockDate) {
        lock.setLockDate(lockDate);
    }

    public boolean isLocked(String owner) {
        return lock.isLocked(owner);
    }

    public boolean isLocked(User owner) {
        return lock.isLocked(owner);
    }

    public boolean isLocked() {
        return lock.isLocked();
    }

    public String getLockOwner() {
        return lock.getLockOwner();
    }

    public void setLockOwner(String username) {
        lock.setLockOwner(username);
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }
}
