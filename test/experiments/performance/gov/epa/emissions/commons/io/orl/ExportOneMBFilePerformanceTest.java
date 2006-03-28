package gov.epa.emissions.commons.io.orl;

import java.io.File;

public class ExportOneMBFilePerformanceTest extends ExportPerformanceTest {

    public ExportOneMBFilePerformanceTest(String name) {
        super(name);
    }

    public void testTrackMemory() throws Exception {
        File importFile = new File("test/data/orl/nc/performance", "onroad-5MB.txt");

        super.doImport(importFile);

        long before = usedMemory();
        super.doExport();
        System.out.println("Memory used: " + (usedMemory() - before) + " MB");
    }

}
