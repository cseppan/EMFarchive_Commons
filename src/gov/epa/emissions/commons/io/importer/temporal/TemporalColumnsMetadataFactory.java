package gov.epa.emissions.commons.io.importer.temporal;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.importer.FileFormat;

import java.util.HashMap;
import java.util.Map;

public class TemporalColumnsMetadataFactory {

    private Map map;

    public TemporalColumnsMetadataFactory(SqlDataTypes sqlType) {
        map = new HashMap();
        map.put("MONTHLY", new MonthlyFileFormat(sqlType));
        map.put("WEEKLY", new WeeklyFileFormat(sqlType));
        map.put("DIURNAL WEEKDAY", new DiurnalFileFormat(sqlType));
        map.put("DIURNAL WEEKEND", new DiurnalFileFormat(sqlType));
    }

    public FileFormat get(String header) {
        return (FileFormat) map.get(header);
    }

}
