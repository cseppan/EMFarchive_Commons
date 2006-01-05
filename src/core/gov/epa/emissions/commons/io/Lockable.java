package gov.epa.emissions.commons.io;

import gov.epa.emissions.commons.security.User;

import java.util.Date;

public interface Lockable {

    Date getLockDate();

    void setLockDate(Date lockDate);

    String getLockOwner();

    void setLockOwner(String owner);

    boolean isLocked(String owner);

    boolean isLocked(User owner);

    boolean isLocked();

}