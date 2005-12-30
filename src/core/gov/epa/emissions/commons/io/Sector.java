package gov.epa.emissions.commons.io;

import gov.epa.emissions.commons.security.User;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class Sector implements Serializable {
    private long id;

    private String name;

    private String description;

    private List sectorCriteria;

    private Mutex lock;

    public Sector() {
        this.sectorCriteria = new ArrayList();
        this.lock = new Mutex();
    }

    public Sector(String description, String name) {
        this();
        this.description = description;
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public SectorCriteria[] getSectorCriteria() {
        return (SectorCriteria[]) this.sectorCriteria.toArray(new SectorCriteria[0]);
    }

    public void setSectorCriteria(SectorCriteria[] sectorCriteria) {
        this.sectorCriteria.clear();
        this.sectorCriteria.addAll(Arrays.asList(sectorCriteria));
    }

    public void addSectorCriteria(SectorCriteria criteria) {
        this.sectorCriteria.add(criteria);
    }

    public Date getLockDate() {
        return lock.getLockDate();
    }

    public void setLockDate(Date lockDate) {
        lock.setLockDate(lockDate);
    }

    public void setSectorCriteria(List sectorCriteria) {
        this.sectorCriteria = sectorCriteria;
    }

    public String getUsername() {
        return lock.getUsername();
    }

    public void setUsername(String username) {
        lock.setUsername(username);
    }

    public boolean isLocked(User user) {
        return lock.isLocked(user);
    }

    public boolean isLocked() {
        return lock.isLocked();
    }
}
