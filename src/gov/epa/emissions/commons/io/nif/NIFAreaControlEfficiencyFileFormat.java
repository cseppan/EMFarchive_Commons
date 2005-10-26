package gov.epa.emissions.commons.io.nif;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.RealFormatter;
import gov.epa.emissions.commons.io.StringFormatter;
import gov.epa.emissions.commons.io.importer.FileFormat;

public class NIFAreaControlEfficiencyFileFormat implements FileFormat {

	private Column[] cols;

	public NIFAreaControlEfficiencyFileFormat(SqlDataTypes types) {
        cols = createCols(types);
    }
	

	public String identify() {
		return "NIF Area Control Efficiency";
	}

	public Column[] cols() {
		return cols; 
	}
	
	private Column[] createCols(SqlDataTypes types) {
		Column recordType = new Column("record_type",types.stringType(2),2,new StringFormatter(2));
		Column state_county_fips = new Column("state_county_fips",types.stringType(5),5, new StringFormatter(5));
		Column scc = new Column("scc",types.stringType(10), 10, new StringFormatter(10));
		Column pollutanCode = new Column("pollutant_code",types.stringType(9), 9, new StringFormatter(9));
		Column controlEff = new Column("control_eff",types.realType(), 5, new RealFormatter()); 
		Column captureEff = new Column("capture_eff",types.realType(), 5, new RealFormatter());
		Column totalEff = new Column("total_eff",types.realType(), 5, new RealFormatter());
		Column priDeviceTypeCode = new Column("pri_device_type_code",types.stringType(4),4, new StringFormatter(4));
		Column secDeviceTypeCode = new Column("sec_device_type_code",types.stringType(4),4, new StringFormatter(4));
		Column controlSystemDesc = new Column("control_system_desc",types.stringType(40),40, new StringFormatter(40));
		Column submittalFlag = new Column("submittal_flag",types.stringType(4),4, new StringFormatter(4));
		Column tribalCode = new Column("tribal_code",types.stringType(4),4, new StringFormatter(4));
		return new Column[]{recordType, state_county_fips, scc, 
				pollutanCode, controlEff, captureEff, totalEff, priDeviceTypeCode, 
				secDeviceTypeCode, controlSystemDesc,
				submittalFlag, tribalCode};
		
	}

}