package gov.epa.emissions.commons.io;

import gov.epa.emissions.commons.security.User;

import java.util.Date;

public class Mutex {

    private String username;

    private Date lockDate;

    public Date getLockDate() {
        return lockDate;
    }

    public void setLockDate(Date lockDate) {
        this.lockDate = lockDate;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public boolean isLocked(User user) {
        return (user.getFullName().equals(username));
    }

    public boolean isLocked() {
        return username != null && lockDate != null;
    }

}
