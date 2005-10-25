package gov.epa.emissions.commons.io.nif;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.RealFormatter;
import gov.epa.emissions.commons.io.StringFormatter;
import gov.epa.emissions.commons.io.importer.FileFormat;

public class NIFAreaPeriodsFileFormat implements FileFormat {

	private Column[] cols;

	public NIFAreaPeriodsFileFormat(SqlDataTypes types) {
		cols = createCols(types);
	}

	public String identify() {
		return "NIF Area Emission Periods";
	}

	public Column[] cols() {
		return cols;
	}

	private Column[] createCols(SqlDataTypes types) {
		Column recordType = new Column("record_type", types.stringType(2), 2,
				new StringFormatter(2));
		Column state_county_fips = new Column("state_county_fips", types
				.stringType(5), 5, new StringFormatter(5));
		Column scc = new Column("scc", types.stringType(10), 10,
				new StringFormatter(10));
		Column startDate = new Column("start_date", types.stringType(8), 8,
				new StringFormatter(8));
		Column endDate = new Column("end_date", types.stringType(8), 8,
				new StringFormatter(8));
		Column spacer1 = new Column("spacer1", types.stringType(2), 2,
				new StringFormatter(2));
		Column start_time = new Column("start_time", types.realType(), 4,
				new RealFormatter());
		Column end_time = new Column("end_time", types.realType(), 4,
				new RealFormatter());
		Column thruput = new Column("thruput", types.realType(), 10,
				new RealFormatter());
		Column thruputUnits = new Column("thruput_units", types.stringType(10),
				10, new StringFormatter(10));
		Column materialCode = new Column("material_code", types.realType(), 4,
				new RealFormatter());
		Column materialIO = new Column("material_io", types.stringType(10), 10,
				new StringFormatter(10));
		Column daysPerWeek = new Column("days_per_week", types.realType(), 1,
				new RealFormatter());
		Column weeksPerPeriod = new Column("weeks_per_period",
				types.realType(), 2, new RealFormatter());
		Column hoursPerDay = new Column("hours_per_day", types.realType(), 2,
				new RealFormatter());
		Column hoursPerPeriod = new Column("hours_per_period",
				types.realType(), 4, new RealFormatter());
		Column submittal_flag = new Column("submittal_flag", types
				.stringType(4), 4, new StringFormatter(4));
		Column tribal_code = new Column("tribal_code", types.stringType(4), 4,
				new StringFormatter(4));

		return new Column[] { recordType, state_county_fips, scc, startDate,
				endDate, spacer1, start_time, end_time, thruput, thruputUnits,
				materialCode, materialIO, daysPerWeek, weeksPerPeriod,
				hoursPerDay, hoursPerPeriod, submittal_flag, tribal_code };
	}

}
