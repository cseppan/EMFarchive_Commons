package gov.epa.emissions.commons.db.postgres;

import java.sql.ResultSet;

import gov.epa.emissions.commons.PerformanceTestCase;

public class FiftyMBFilePostgresQueryPerformanceTest extends PerformanceTestCase {

    public FiftyMBFilePostgresQueryPerformanceTest(String name) {
        super(name);
    }

    public void testTrackMemory() throws Exception {
        startTracking();

        int count = 341374;

        OptimizedPostgresQuery runner = new OptimizedPostgresQuery(emissions().getConnection());
        runner.init("SELECT * FROM emissions.test_onroad_fifty_mb", 50000);

        for (int i = 0; i < count;) {
            ResultSet rs = runner.execute();
            while(rs.next()){
                rs.getObject(1);
            }
            
            i += 50000;
        }

        dumpStats();
    }

}
