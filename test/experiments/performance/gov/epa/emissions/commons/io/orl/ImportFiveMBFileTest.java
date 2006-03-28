package gov.epa.emissions.commons.io.orl;

import java.io.File;

public class ImportFiveMBFileTest extends ImportPerformanceTestCase {

    public ImportFiveMBFileTest(String name) {
        super(name);
    }

    public void testImport() throws Exception {
        File importFile = new File("test/data/orl/nc/performance", "onroad-5MB.txt");
        
        long before = time();
        super.doImport(importFile, "onroad_five_mb");
        System.out.println("Import of 5 MB complete in " + (time() - before));
    }

}
