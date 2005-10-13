package gov.epa.emissions.commons.io.importer.ida;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;

public class IDAActivityColumnsMetadata implements ColumnsMetadata {

	private String[] colTypes;

	private String[] colNames;

	public IDAActivityColumnsMetadata(String[] pollutants, SqlDataTypes dataTypes) {
		String intType = dataTypes.intType();
		String[] desColTypes = new String[] { intType, intType,
				dataTypes.stringType(10),dataTypes.stringType(10)};
		String[] desColNames = new String[] { "STID", "CYID", "LINK_ID", "SCC" };
		colTypes = addPollTypes(pollutants.length, desColTypes, dataTypes);
		colNames = addPollNames(pollutants, desColNames);
	}

	public int[] widths() {
		return null;
	}

	public String[] colTypes() {
		return colTypes;
	}

	public String[] colNames() {
		return colNames;
	}

	private String[] addPollTypes(int noOfPollutants, String[] array,
			SqlDataTypes dataTypes) {
		List types = new ArrayList();
		types.addAll(Arrays.asList(array));
		for (int i = 0; i < noOfPollutants; i++) {
			types.add(dataTypes.realType());
			types.add(dataTypes.realType());
		}
		return (String[]) types.toArray(new String[0]);
	}

	private String[] addPollNames(String[] pollutants, String[] desColNames) {
		List names = new ArrayList();
		names.addAll(Arrays.asList(desColNames));
		for (int i = 0; i < pollutants.length; i++) {
			names.add(pollutants[i]);
		}
		return (String[]) names.toArray(new String[0]);
	}

}
