package gov.epa.emissions.commons.io.importer.ida;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;

public class IDAAreaColumnsMetadata implements ColumnsMetadata {

	private String[] colTypes;

	private String[] colNames;

	private int[] widths;

	public IDAAreaColumnsMetadata(String[] pollutants, SqlDataTypes dataTypes) {
		String intType = dataTypes.intType();
		String[] desColTypes = new String[] { intType, intType,
				dataTypes.stringType(10) };
		String[] desColNames = new String[] { "STID", "CYID", "SCC" };
		int[] desColWidths = new int[] { 2, 3, 10 };
		colTypes = addPollTypes(pollutants.length, desColTypes, dataTypes);
		colNames = addPollNames(pollutants, desColNames);
		widths = addPollWidths(pollutants.length, desColWidths);

	}

	public int[] widths() {
		return widths;
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
			types.add(dataTypes.realType());
			types.add(dataTypes.realType());
			types.add(dataTypes.realType());
			types.add(dataTypes.realType());
		}
		return (String[]) types.toArray(new String[0]);
	}

	private String[] addPollNames(String[] pollutants, String[] desColNames) {
		List names = new ArrayList();
		names.addAll(Arrays.asList(desColNames));
		for (int i = 0; i < pollutants.length; i++) {
			names.add("ANN_" + pollutants[i]);
			names.add("AVD_" + pollutants[i]);
			names.add("EMF_" + pollutants[i]);
			names.add("CE_" + pollutants[i]);
			names.add("RE_" + pollutants[i]);
			names.add("RP_" + pollutants[i]);
		}
		return (String[]) names.toArray(new String[0]);
	}

	private int[] addPollWidths(int length, int[] desColWidths) {
		int resolution = 6;
		int totalCols = desColWidths.length + resolution * length;
		int[] widths = new int[totalCols];
		for (int i = 0; i < desColWidths.length; i++) {
			widths[i] = desColWidths[i];
		}
		for (int i = 0; i < length; i ++) {
			int startIndex = desColWidths.length+i*resolution;
			widths[startIndex	 ] = 10;
			widths[startIndex + 1] = 10;
			widths[startIndex + 2] = 11;
			widths[startIndex + 3] = 7;
			widths[startIndex + 4] = 3;
			widths[startIndex + 5] = 6;
		}
		return widths;
	}

    public String identify() {
        return "IDA Area";
    }

}
