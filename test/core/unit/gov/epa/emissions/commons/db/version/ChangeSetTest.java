package gov.epa.emissions.commons.db.version;

import junit.framework.TestCase;

public class ChangeSetTest extends TestCase {

    public void testShouldBeAbleToAddMultipleDeleted() {
        ChangeSet changeset = new ChangeSet();
        VersionedRecord[] records = { new VersionedRecord(), new VersionedRecord() };

        changeset.addDeleted(records);

        VersionedRecord[] results = changeset.getDeletedRecords();
        assertEquals(records.length, results.length);
        for (int i = 0; i < results.length; i++) {
            assertEquals(records[i], results[i]);
        }
    }

    public void testShouldBeAbleToClearChanges() {
        ChangeSet changeset = new ChangeSet();

        changeset.addDeleted(new VersionedRecord());
        assertEquals(1, changeset.getDeletedRecords().length);
        changeset.clear();
        assertEquals(0, changeset.getDeletedRecords().length);

        changeset.addNew(new VersionedRecord());
        assertEquals(1, changeset.getNewRecords().length);
        changeset.clear();
        assertEquals(0, changeset.getNewRecords().length);

        changeset.addUpdated(new VersionedRecord());
        assertEquals(1, changeset.getUpdatedRecords().length);
        changeset.clear();
        assertEquals(0, changeset.getUpdatedRecords().length);
    }

    public void testShouldReturnTrueIfItContainsUpdatedRecord() {
        ChangeSet changeset = new ChangeSet();
        VersionedRecord record = new VersionedRecord();

        changeset.addUpdated(record);

        assertTrue("Should contain the updated record", changeset.containsUpdated(record));
    }

    public void testShouldReturnTrueIfItContainsNewRecord() {
        ChangeSet changeset = new ChangeSet();
        VersionedRecord record = new VersionedRecord();

        changeset.addNew(record);

        assertTrue("Should contain the new record", changeset.containsNew(record));
    }

    public void testShouldConfirmAvailabilityOfChangesIfItContainsEitherNewDeletedOrUpdatedRecords() {
        ChangeSet newCS = new ChangeSet();
        newCS.addNew(new VersionedRecord());
        assertTrue("Adding a New record should confirm availability of 'changes'", newCS.hasChanges());

        ChangeSet deletedCS = new ChangeSet();
        deletedCS.addDeleted(new VersionedRecord());
        assertTrue("Deleting a record should confirm availability of 'changes'", deletedCS.hasChanges());

        ChangeSet updatedCS = new ChangeSet();
        updatedCS.addUpdated(new VersionedRecord());
        assertTrue("Updating a record should confirm availability of 'changes'", updatedCS.hasChanges());

        ChangeSet newDeleteCS = new ChangeSet();
        newDeleteCS.addNew(new VersionedRecord());
        newDeleteCS.addDeleted(new VersionedRecord());
        assertTrue("Adding and Deleting records should confirm availability of 'changes'", newDeleteCS.hasChanges());

    }
}
