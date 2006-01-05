package gov.epa.emissions.commons.io;

import gov.epa.emissions.commons.security.User;

import java.util.Date;

public class Mutex {

    private String owner;

    private Date lockDate;

    public Date getLockDate() {
        return lockDate;
    }

    public void setLockDate(Date lockDate) {
        this.lockDate = lockDate;
    }

    public String getLockOwner() {
        return owner;
    }

    public void setLockOwner(String username) {
        this.owner = username;
    }

    public boolean isLocked(User user) {
        return (user.getFullName().equals(owner));
    }

    public boolean isLocked() {
        return owner != null && lockDate != null;
    }

}
