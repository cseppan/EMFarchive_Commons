package gov.epa.emissions.commons.io.nif.point;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.importer.FileFormat;

public class NIFPointControlEquipmentFileFormat implements FileFormat {

    private Column[] cols;

    public NIFPointControlEquipmentFileFormat(SqlDataTypes types) {
        cols = createCols(types);
    }

    public String identify() {
        return "NIF3.0 Point Control Efficiency";
    }

    public Column[] cols() {
        return cols; 
    }
    
    private Column[] createCols(SqlDataTypes types) {
        String [] names = { "record_type", "state_county_fips",
                            "state_facility_id", "emission_unit_id",
                            "emission_process_id","pollutant_code",
                            "spacer1", "control_eff",
                            "capture_eff", "total_eff", 
                            "pri_device_type_code", "sec_device_type_code",
                            "spacer2", "control_system_desc",
                            "ter_device_type_code", "qua_device_type_code", 
                            "submittal_flag", "tribal_code"};
        
        String[] colTypes = { "C", "C", "C", "C", "C", "C", "C", "N", "N", "N", "C", "C", "C", "C", "C", "C", "C", "C"};
        int [] widths ={2, 5, 15, 6, 6, 9, 11, 5, 5, 5, 4, 4, 25, 40, 4, 4, 4, 4};
        
        System.err.println("Control Equipment - names.length=" + names.length + ", colTypes.length="
                + colTypes.length + ", widths.length=" + widths.length);
        NIFPointFileFormat format = new NIFPointFileFormat(types);
        return format.createCols(names,colTypes,widths);
    }
    
}
