package gov.epa.emissions.commons.io.importer.temporal;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;

import java.util.HashMap;
import java.util.Map;

public class TemporalColumnsMetadataFactory {

    private Map map;

    public TemporalColumnsMetadataFactory(SqlDataTypes sqlType) {
        map = new HashMap();
        map.put("MONTHLY", new MonthlyColumnsMetadata(sqlType));
        map.put("WEEKLY", new WeeklyColumnsMetadata(sqlType));
        map.put("DIURNAL WEEKDAY", new DiurnalColumnsMetadata(sqlType));
        map.put("DIURNAL WEEKEND", new DiurnalColumnsMetadata(sqlType));
    }

    public ColumnsMetadata get(String header) {
        return (ColumnsMetadata) map.get(header);
    }

}
