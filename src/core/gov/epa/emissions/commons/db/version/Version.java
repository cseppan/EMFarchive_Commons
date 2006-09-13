package gov.epa.emissions.commons.db.version;

import gov.epa.emissions.commons.data.Lockable;
import gov.epa.emissions.commons.data.Mutex;
import gov.epa.emissions.commons.security.User;

import java.io.Serializable;
import java.util.Date;

public class Version implements Lockable, Serializable {

    private int id;

    private int datasetId;

    private int version;

    /*parent versions*/
    private String path;

    private boolean finalVersion = false;

    private String name;

    private Date lastModifiedDate;

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

    public void setDatasetId(int datasetId) {
        this.datasetId = datasetId;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getDatasetId() {
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
     * create path that includes 'me'
     */
    public String createCompletePath() {
        return (path == null || path.length() == 0) ? (version + "") : (path + "," + version);
    }

    public long getBase() {
        if (version == 0)// i.e. root
            return 0;

        int start = path.lastIndexOf(",") + 1;
        return Long.parseLong(path.substring(start));
    }

    public void setLastModifiedDate(Date lastModifiedDate) {
        this.lastModifiedDate = lastModifiedDate;
    }

    public Date getLastModifiedDate() {
        return lastModifiedDate;
    }

    public void setCreator(User creator) {
        this.creator = creator;
    }

    public User getCreator() {
        return creator;
    }

    public String toString() {
        return version + " (" + name + ")";
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

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public boolean equals(Object other) {
        return (other instanceof Version) && (((Version) other).id == id);
    }

    public int hashCode() {
        return id;
    }
}
