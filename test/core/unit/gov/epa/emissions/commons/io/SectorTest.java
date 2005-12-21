package gov.epa.emissions.commons.io;

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
}
