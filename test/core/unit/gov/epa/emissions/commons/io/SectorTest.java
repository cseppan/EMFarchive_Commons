package gov.epa.emissions.commons.io;

import gov.epa.emissions.commons.security.User;

import java.util.Date;

import junit.framework.TestCase;

public class SectorTest extends TestCase {

    public void testAddSectorCriteria() {
        Sector s = new Sector();
        s.addSectorCriteria(new SectorCriteria());

        assertEquals(1, s.getSectorCriteria().length);
    }

    public void testSetSectorCriteria() {
        Sector s = new Sector();
        s.addSectorCriteria(new SectorCriteria());
        assertEquals(1, s.getSectorCriteria().length);

        SectorCriteria c1 = new SectorCriteria();
        SectorCriteria c2 = new SectorCriteria();
        s.setSectorCriteria(new SectorCriteria[] { c1, c2 });

        assertEquals(2, s.getSectorCriteria().length);
        assertEquals(c1, s.getSectorCriteria()[0]);
        assertEquals(c2, s.getSectorCriteria()[1]);
    }

    public void testShouldBeLockedOnlyIfUsernameAndDateIsSet() {
        Sector locked = new Sector();
        locked.setUsername("user");
        locked.setLockDate(new Date());
        assertTrue("Should be locked", locked.isLocked());

        Sector unlockedAsOnlyUsernameIsSet = new Sector();
        unlockedAsOnlyUsernameIsSet.setUsername("user");
        assertFalse("Should be unlocked", unlockedAsOnlyUsernameIsSet.isLocked());

        Sector unlockedAsOnlyLockedDateIsSet = new Sector();
        unlockedAsOnlyLockedDateIsSet.setLockDate(new Date());
        assertFalse("Should be unlocked", unlockedAsOnlyLockedDateIsSet.isLocked());
    }

    public void testShouldBeLockedIfUsernameMatches() throws Exception {
        Sector locked = new Sector();
        locked.setUsername("user");
        locked.setLockDate(new Date());

        User lockedByUser = new User();
        lockedByUser.setFullName("user");
        assertTrue("Should be locked", locked.isLocked(lockedByUser));
        
        User notLockedByUser = new User();
        notLockedByUser.setFullName("user2");
        assertFalse("Should not be locked", locked.isLocked(notLockedByUser));
    }
}
