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
}
