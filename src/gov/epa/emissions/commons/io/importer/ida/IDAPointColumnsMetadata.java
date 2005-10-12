package gov.epa.emissions.commons.io.importer.ida;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;

public class IDAPointColumnsMetadata implements ColumnsMetadata {

	private String[] colTypes;

	private String[] colNames;

	private int[] widths;

	public IDAPointColumnsMetadata(String[] pollutants, SqlDataTypes dataTypes) {
		String intType = dataTypes.intType();
		String realType = dataTypes.realType();

		String[] desColNames = new String[] { "STID", "CYID", "PLANTID",
				"POINTID", "STACKID", "ORISID", "BLRID", "SEGMENT", "PLANT",
				"SCC", "BEGYR", "ENDYR", "STKHGT", "STKDIAM", "STKTEMP",
				"STKFLOW", "STKVEL", "BOILCAP", "CAPUNITS", "WINTHRU",
				"SPRTHRU", "SUMTHRU", "FALTHRU", "HOURS", "START", "DAYS",
				"WEEKS", "THRUPUT", "MAXRATE", "HEATCON", "SULFCON", "ASHCON",
				"NETDC", "SIC", "LATC", "LONC", "OFFSHORE" };
		int[] desColWidths = new int[] { 2, 3, 15, 15, 12, 6, 6, 2, 40, 10, 4,
				4, 4, 6, 4, 10, 9, 8, 1, 2, 2, 2, 2, 2, 2, 1, 2, 11, 12, 8, 5,
				5, 9, 4, 9, 9, 1 };
		
		String[] desColTypes = new String[] {
				intType, intType,
				dataTypes.stringType(15),dataTypes.stringType(15),
				dataTypes.stringType(12),dataTypes.stringType(6),
				dataTypes.stringType(6),dataTypes.stringType(2),
				dataTypes.stringType(40),dataTypes.stringType(10),
				intType,intType,
				realType,realType,realType,realType,realType,realType,
				dataTypes.stringType(2),
				realType,realType,realType,realType,
				intType,intType,intType,intType,
				realType,realType,realType,realType,realType,realType,
				intType,
				realType, realType, dataTypes.stringType(2)
		};				
				
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
			types.add(dataTypes.intType());
			types.add(dataTypes.intType());
		}
		return (String[]) types.toArray(new String[0]);
	}

	private String[] addPollNames(String[] pollutants, String[] desColNames) {
		List names = new ArrayList();
		names.addAll(Arrays.asList(desColNames));
		for (int i = 0; i < pollutants.length; i++) {
			names.add("ANN_" + pollutants[i]);
			names.add("AVD_" + pollutants[i]);
			names.add("CE_" + pollutants[i]);
			names.add("RE_" + pollutants[i]);
			names.add("EMF_" + pollutants[i]);
			names.add("CPRI_" + pollutants[i]);
			names.add("CSEC_" + pollutants[i]);
		}
		return (String[]) names.toArray(new String[0]);
	}

	private int[] addPollWidths(int length, int[] desColWidths) {
		int resolution = 7;
		int totalCols = desColWidths.length + resolution * length;
		int[] widths = new int[totalCols];
		for (int i = 0; i < desColWidths.length; i++) {
			widths[i] = desColWidths[i];
		}
		for (int i = 0; i < length; i++) {
			int startIndex = desColWidths.length + i * resolution;
			widths[startIndex] = 13;
			widths[startIndex + 1] = 13;
			widths[startIndex + 2] = 7;
			widths[startIndex + 3] = 3;
			widths[startIndex + 4] = 10;
			widths[startIndex + 5] = 3;
			widths[startIndex + 6] = 3;
		}
		return widths;
	}

}
