package gov.epa.emissions.commons.db.version;

import junit.framework.TestCase;

public class VersionTest extends TestCase {

    public void testPathForVersionDerivedFromRootVersion() {
        Version v = new Version();
        v.setVersion(0);
        v.setPath("");
        
        assertEquals("0", v.createPathForDerived());
    }
    
    public void testPathForVersionDerivedFromNonRootVersion() {
        Version v = new Version();
        v.setVersion(2);
        v.setPath("0,1");
        
        assertEquals("0,1,2", v.createPathForDerived());
    }
    
    public void testShouldObtainBaseFromPath() {
        Version v2 = new Version();
        v2.setVersion(2);
        v2.setPath("0,1");
        assertEquals(1, v2.getBase());
        
        Version v12 = new Version();
        v12.setVersion(12);
        v12.setPath("0,1,11");
        assertEquals(11, v12.getBase());
    }
    
    
}
