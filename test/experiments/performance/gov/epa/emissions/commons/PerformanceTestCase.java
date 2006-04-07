package gov.epa.emissions.commons;

import java.util.Date;

import gov.epa.emissions.commons.io.importer.PersistenceTestCase;

public abstract class PerformanceTestCase extends PersistenceTestCase {

    private long startMemory;

    private long startTime;

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

    protected long maxMemory() {
        return (Runtime.getRuntime().maxMemory() / megabyte());
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

    protected long time() {
        return new Date().getTime() / 1000;
    }
    

    protected void startTracking() {
        startMemory = usedMemory();
        startTime = time();
    }
    

    protected void dumpStats() {
        long current = usedMemory();
        System.out.println("Time: " + (time() - startTime) + " secs using " + (current - startMemory) + " MB memory "
                + "(current:" + current + ", start: " + startMemory + ")");
    }

}
