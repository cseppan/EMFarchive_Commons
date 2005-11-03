package gov.epa.emissions.commons.io.nif.point;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.importer.FileFormat;

public class NIFPointEmissionProcessFileFormat implements FileFormat {

    private Column[] cols;

    public NIFPointEmissionProcessFileFormat(SqlDataTypes types) {
        cols = createCols(types);
    }

    public String identify() {
        return "NIF3.0 Point Emission Process";
    }

    public Column[] cols() {
        return cols; 
    }
    
    private Column[] createCols(SqlDataTypes types) {
        String [] names = {"record_type", "state_county_fips", "state_facility_id", "emission_unit_id", "emission_point_id", "emission_process_id", "scc", "mact_code", "process_desc", "winter_thruput_pct", "spring_thruput_pct", "summer_thruput_pct", "fall_thruput_pct", "avg_days_per_week", "avg_weeks_per_year", "avg_hours_per_day", "avg_hours_per_year", "heat_content", "sulfur_content", "ash_content", "mact_compliance", "submittal_flag", "tribal_code"};
        String [] colTypes = {"C", "C", "C", "C", "C", "C", "C", "C", "C", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "C", "C", "C"};
        int [] widths = {2, 5, 15, 6, 6, 6, 10, 6, 78, 3, 3, 3, 3, 1, 2, 2, 4, 9, 5, 5, 6, 4, 4};

        System.err.println("Emission Process - names.length=" + names.length + ", colTypes.length=" + colTypes.length
                + ", widths.length=" + widths.length);
        NIFPointFileFormat format = new NIFPointFileFormat(types);
        return format.createCols(names,colTypes,widths);
    }
    
}
