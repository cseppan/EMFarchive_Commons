package gov.epa.emissions.commons.io;

import gov.epa.emissions.commons.security.User;

import java.util.Date;

public interface Lockable {

    Date getLockDate();

    void setLockDate(Date lockDate);

    String getLockOwner();

    void setLockOwner(String username);

    boolean isLocked(User user);

    boolean isLocked();

}