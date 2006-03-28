package gov.epa.emissions.commons;

import gov.epa.emissions.commons.io.importer.PersistenceTestCase;

public abstract class PerformanceTestCase extends PersistenceTestCase {

    public PerformanceTestCase(String name) {
        super(name);
    }

    protected void doTearDown() throws Exception {
        // TODO Auto-generated method stub

    }

    protected void dumpMemory() {
        System.out.println(usedMemory() + " MB");
    }

    protected long usedMemory() {
        return (totalMemory() - freeMemory());
    }

    protected long freeMemory() {
        return Runtime.getRuntime().freeMemory() / megabyte();
    }

    private int megabyte() {
        return (1024 * 1024);
    }

    protected long totalMemory() {
        return Runtime.getRuntime().totalMemory() / megabyte();
    }
}
