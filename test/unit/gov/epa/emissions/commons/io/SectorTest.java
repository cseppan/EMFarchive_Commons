package gov.epa.emissions.commons.io;

import junit.framework.TestCase;

public class SectorTest extends TestCase {

    public void testAddSectorCriteria() {
        Sector s  = new Sector();
        s.addSectorCriteria(new SectorCriteria());
        
        assertEquals(1, s.getSectorCriteria().length);
    }
}
