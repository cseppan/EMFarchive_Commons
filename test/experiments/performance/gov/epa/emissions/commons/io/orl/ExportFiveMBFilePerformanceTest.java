package gov.epa.emissions.commons.io.orl;

import java.io.File;

public class ExportFiveMBFilePerformanceTest extends ExportPerformanceTest {

    public ExportFiveMBFilePerformanceTest(String name) {
        super(name);
    }

    public void testTrackMemory() throws Exception {
        File importFile = new File("test/data/orl/nc/performance", "onroad-1MB.txt");

        super.doImport(importFile);

        long before = usedMemory();
        super.doExport();
        System.out.println("Memory used: " + (usedMemory() - before) + " MB");
    }

}
