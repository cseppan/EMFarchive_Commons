package gov.epa.emissions.commons.db.postgres;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.clarkware.junitperf.ConstantTimer;
import com.clarkware.junitperf.LoadTest;
import com.clarkware.junitperf.Timer;

public class FiftyMBFileMultiUserPerformanceTests {

    public static Test suite() {
        TestSuite suite = new TestSuite();

        int users = 3;
        Timer timer = new ConstantTimer(1000);

        Test smallFile = new LoadTest(new FiftyMBFilePostgresQueryPerformanceTest("testTrackMemory"), users, timer);
        suite.addTest(smallFile);

        return suite;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}
