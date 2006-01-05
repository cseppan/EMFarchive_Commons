package gov.epa.emissions.commons.io;

import gov.epa.emissions.commons.security.User;

import java.util.Date;

import junit.framework.TestCase;

public class DatasetTypeTest extends TestCase {

    public void testAddKeyword() {
        DatasetType type = new DatasetType();
        Keyword kw = new Keyword();
        kw.setName("key1");
        type.addKeyword(kw);

        Keyword[] actual = type.getKeywords();
        assertEquals(1, actual.length);
        assertEquals("key1", actual[0].getName());
    }

    public void testSetKeywords() {
        DatasetType type = new DatasetType();
        Keyword kw1 = new Keyword("key1");
        Keyword kw2 = new Keyword("key2");

        type.setKeywords(new Keyword[] { kw1, kw2 });

        Keyword[] actual = type.getKeywords();
        assertEquals(2, actual.length);
        assertEquals("key1", actual[0].getName());
        assertEquals("key2", actual[1].getName());
    }

    public void testShouldBeLockedOnlyIfUsernameAndDateIsSet() {
        DatasetType locked = new DatasetType();
        locked.setLockOwner("user");
        locked.setLockDate(new Date());
        assertTrue("Should be locked", locked.isLocked());

        DatasetType unlockedAsOnlyUsernameIsSet = new DatasetType();
        unlockedAsOnlyUsernameIsSet.setLockOwner("user");
        assertFalse("Should be unlocked", unlockedAsOnlyUsernameIsSet.isLocked());

        DatasetType unlockedAsOnlyLockedDateIsSet = new DatasetType();
        unlockedAsOnlyLockedDateIsSet.setLockDate(new Date());
        assertFalse("Should be unlocked", unlockedAsOnlyLockedDateIsSet.isLocked());
    }

    public void testShouldBeLockedIfUsernameMatches() throws Exception {
        DatasetType locked = new DatasetType();
        locked.setLockOwner("user");
        locked.setLockDate(new Date());

        User lockedByUser = new User();
        lockedByUser.setFullName("user");
        assertTrue("Should be locked", locked.isLocked(lockedByUser));

        User notLockedByUser = new User();
        notLockedByUser.setFullName("user2");
        assertFalse("Should not be locked", locked.isLocked(notLockedByUser));
    }
}
