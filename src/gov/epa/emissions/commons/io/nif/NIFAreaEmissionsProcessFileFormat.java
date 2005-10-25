package gov.epa.emissions.commons.io.nif;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.RealFormatter;
import gov.epa.emissions.commons.io.StringFormatter;
import gov.epa.emissions.commons.io.importer.FileFormat;

public class NIFAreaEmissionsProcessFileFormat implements FileFormat {

	private Column[] cols;

	public NIFAreaEmissionsProcessFileFormat(SqlDataTypes types) {
		cols = createCols(types);
	}

	public String identify() {
		return "NIF Area Emission Process";
	}

	public Column[] cols() {
		return cols;
	}

	private Column[] createCols(SqlDataTypes types) {
		Column recordType = new Column("record_type", types.stringType(2), 2,
				new StringFormatter(2));
		Column stateCountyFips = new Column("state_county_fips", types
				.stringType(5), 5, new StringFormatter(5));
		Column scc = new Column("scc", types.stringType(10), 10,
				new StringFormatter(10));
		Column mactCode = new Column("mact_code", types.stringType(6), 6,
				new StringFormatter(6));
		Column processDesc = new Column("process_desc", types.stringType(78),
				78, new StringFormatter(78));
		Column sicCode = new Column("sic_code", types.stringType(4), 4,
				new StringFormatter(4));
		Column naicsCode = new Column("naics_code", types.stringType(6), 6,
				new StringFormatter(6));
		Column winterThruputPct = new Column("winter_thruput_pct", types
				.realType(), 3, new RealFormatter());
		Column springThruputPct = new Column("spring_thruput_pct", types
				.realType(), 3, new RealFormatter());
		Column summerThruputPct = new Column("summer_thruput_pct", types
				.realType(), 3, new RealFormatter());
		Column fallThruputPct = new Column("fall_thruput_pct",
				types.realType(), 3, new RealFormatter());
		Column avgDaysPerWeek = new Column("avg_days_per_week", types
				.realType(), 1, new RealFormatter());
		Column avgWeeksPerYear = new Column("avg_weeks_per_year", types
				.realType(), 2, new RealFormatter());
		Column avgHoursPerDay = new Column("avg_hours_per_day", types
				.realType(), 2, new RealFormatter());
		Column avgHoursPerYear = new Column("avg_hours_per_year", types
				.realType(), 4, new RealFormatter());
		Column heatContent = new Column("heat_content", types.realType(), 8,
				new RealFormatter());
		Column sulfurContent = new Column("sulfur_content", types.realType(),
				5, new RealFormatter());
		Column ashContent = new Column("ash_content", types.realType(), 5,
				new RealFormatter());
		Column mactCompliance = new Column("mact_compliance", types
				.stringType(6), 6, new StringFormatter(6));
		Column submittalFlag = new Column("submittal_flag",
				types.stringType(4), 4, new StringFormatter(4));
		Column tribalCode = new Column("tribal_code", types.stringType(4), 4,
				new StringFormatter(4));
		return new Column[] { recordType, stateCountyFips, scc, mactCode,
				processDesc, sicCode, naicsCode, winterThruputPct,
				springThruputPct, summerThruputPct, fallThruputPct,
				avgDaysPerWeek, avgWeeksPerYear, avgHoursPerDay,
				avgHoursPerYear, heatContent, sulfurContent, ashContent,
				mactCompliance, submittalFlag, tribalCode };

	}

}
