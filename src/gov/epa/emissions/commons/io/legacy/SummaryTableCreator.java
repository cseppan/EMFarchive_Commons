package gov.epa.emissions.commons.io.legacy;

import gov.epa.emissions.commons.db.DataQuery;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.TableDefinition;
import gov.epa.emissions.commons.io.DatasetType;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class SummaryTableCreator {
    public static final String AREA_SUMMARY = "nei_area_summary";

    public static final String POINT_SUMMARY = "nei_point_summary";

    public static final String STATE = "State";

    public static final String COUNTY = "County";

    public static final String SCC = "SCC";

    public static final String SCC_DESC = "scc_desc";

    public static final String FIPS = "FIPS";

    public static final String FACILITY = "Facility";

    public static final String UNIT = "Unit";

    public static final String PROCESS = "Process";

    public static final String POINT = "Point";

    public static final String SIC = "SIC";

    public static final String NAICS = "NAICS";

    public static final String MACT = "MACT";

    public static final String HEIGHT = "Height";

    public static final String DIAMETER = "Diameter";

    public static final String EXIT_TEMP = "Exit_Temp";

    public static final String EXIT_VEL = "Exit_Vel";

    public static final String EXIT_FLOW_RATE = "Exit_Flow_Rate";

    public static final String X_COORD = "X_Coord";

    public static final String Y_COORD = "Y_Coord";

    public static final String EMISSION_COL = "emission_value";

    public static final String POLLUTANT_COL = "pollutant_code";

    public static final String STATE_COL = "state_abbr";

    public static final String COUNTY_COL = "County";

    public static final String SCC_COL = "scc";

    public static final String FIPS_COL = "state_county_fips";

    public static final String FACILITY_COL = "state_facility_id";

    public static final String UNIT_COL = "emission_unit_id";

    public static final String PROCESS_COL = "emission_process_id";

    public static final String POINT_COL = "emission_point_id";

    public static final String SIC_COL = "unit_sic_code";

    public static final String NAICS_COL = "unit_naics_code";

    public static final String SIC_AREA_COL = "sic_code";

    public static final String NAICS_AREA_COL = "naics_code";

    public static final String MACT_COL = "mact_code";

    public static final String HEIGHT_COL = "stack_height";

    public static final String DIAMETER_COL = "stack_diameter";

    public static final String EXIT_TEMP_COL = "exit_gas_temp";

    public static final String EXIT_VEL_COL = "exit_gas_velocity";

    public static final String EXIT_FLOW_RATE_COL = "exit_gas_flow_rate";

    public static final String X_COORD_COL = "x_coordinate";

    public static final String Y_COORD_COL = "y_coordinate";

    public static final String START_DATE_COL = "start_date";

    public static final String END_DATE_COL = "end_date";

    public static final String EP_KEY_INDEX = "ep_key";

    public static final String EU_KEY_INDEX = "eu_key";

    public static final String ER_KEY_INDEX = "er_key";

    // ORL columns

    private static final String CAS_COL = ORLDataFormat.CAS_NAME;

    private static final String ANNUAL_EMISSION_COL = ORLDataFormat.ANNUAL_EMISSIONS_NAME;

    private static final String AVERAGE_DAY_EMISSION_COL = ORLDataFormat.AVERAGE_DAY_EMISSIONS_NAME;

    private static final String SCC_COL_ORL = "SCC";// ORLDataFormat.SOURCE_CLASSIFICATION_CODE_NAME;

    private static final String FIPS_COL_ORL = ORLDataFormat.FIPS_NAME;

    private static final String FIPS_COL_REF = "state_county_fips";

    private static final String PLANTID_COL = ORLPointDataFormat.PLANT_ID_CODE_NAME;

    private static final String PLANTID = "Plant_Id";

    private static final String POINTID_COL = ORLPointDataFormat.POINT_ID_CODE_NAME;

    private static final String POINTID = "Point_Id";

    private static final String STACKID_COL = ORLPointDataFormat.STACK_ID_CODE_NAME;

    private static final String STACKID = "Stack_Id";

    private static final String SEGMENT_COL = ORLPointDataFormat.DOE_PLANT_ID_NAME;

    private static final String SEGMENT = "Segment";

    private static final String PLANT_COL = ORLPointDataFormat.PLANT_NAME_NAME;

    private static final String PLANT = "Plant";

    private static final String ERPTYPE_COL = ORLPointDataFormat.EMISSIONS_RELEASE_POINT_TYPE_NAME;

    private static final String ERPTYPE = "Emissons_Release_Point";

    private static final String SRCTYPE_COL = "SRCTYPE";// Point & Nonpoint

    private static final String SRCTYPE = "Source_Type";

    private static final String HEIGHT_COL_ORL = ORLPointDataFormat.STACK_HEIGHT_NAME;

    private static final String DIAMETER_COL_ORL = ORLPointDataFormat.STACK_DIAMETER_NAME;

    private static final String EXIT_TEMP_COL_ORL = ORLPointDataFormat.STACK_GAS_EXIT_TEMPERATURE_NAME;

    private static final String EXIT_FLOW_RATE_COL_ORL = ORLPointDataFormat.STACK_GAS_FLOW_RATE_NAME;

    private static final String EXIT_VEL_COL_ORL = ORLPointDataFormat.STACK_GAS_EXIT_VELOCITY_NAME;

    private static final String SIC_COL_ORL = "SIC";// Point & Nonpoint

    private static final String MACT_COL_ORL = "MACT";// Point & Nonpoint

    private static final String NAICS_COL_ORL = "NAICS";// Point & Nonpoint

    private static final String CTYPE_COL = ORLPointDataFormat.COORDINATE_SYSTEM_TYPE_NAME;

    private static final String CTYPE = "Coordinate_System_Type";

    private static final String X_COORD_COL_ORL = ORLPointDataFormat.X_LOCATION_NAME;

    private static final String Y_COORD_COL_ORL = ORLPointDataFormat.Y_LOCATION_NAME;

    private static final String UMTZ_COL = ORLPointDataFormat.UTM_ZONE_NAME;

    private static final String UMTZ = "UTM_Zone";

    private Datasource emissionsDatasource;

    private Datasource referenceDatasource;

    private ORLDatasetTypesFactory types;

    public SummaryTableCreator(Datasource emissions, Datasource reference, ORLDatasetTypesFactory typesFactory) {
        this.emissionsDatasource = emissions;
        this.referenceDatasource = reference;

        this.types = typesFactory;
    }

    private String clean(String dirtyStr) {
        return dirtyStr.replace('-', '_');
    }

    public void createORLSummaryTable(DatasetType datasetType, String orlTable, String summaryTable,
            boolean annualNotAverageDaily) throws Exception {
        String qualifiedSummaryTable = emissionsDatasource.getName() + "." + summaryTable;
        String qualifiedOrlTable = emissionsDatasource.getName() + "." + orlTable;

        // connect to emissions database
        // get the pollutant CAS codes
        DataQuery emissionsQuery = emissionsDatasource.query();
        String casQuery = "SELECT DISTINCT(" + CAS_COL + ") FROM " + qualifiedOrlTable;
        ResultSet rs = emissionsQuery.executeQuery(casQuery);
        rs.last();
        int numOfPollutants = rs.getRow();
        String[] pollutants = new String[numOfPollutants];
        rs.first();
        for (int index = 0; index < pollutants.length; index++) {
            pollutants[index] = rs.getString(CAS_COL);
            rs.next();
        }
        rs.close();

        // we only want the primary column in the summary table
        String emissionCol = null;
        if (annualNotAverageDaily) {
            emissionCol = ANNUAL_EMISSION_COL;
        } else {
            emissionCol = AVERAGE_DAY_EMISSION_COL;
        }

        // rather than continuously checking for dataset type,
        // check once and set all necessary variables and parameters
        String summarySelectDistinct = null;
        String summaryFromSelectDistinct = " FROM " + referenceDatasource.getName() + ".fips as f, ";
        String tempSelectDistinct = null;
        String tempFromSelectDistinct = null;
        ORLDatasetTypesFactory orlTypes = new DefaultORLDatasetTypesFactory();
        if (datasetType.equals(orlTypes.nonPoint()) || datasetType.equals(types.nonRoad())
                || datasetType.equals(types.onRoad())) {
            // String createIndex = " (INDEX orl_key (" + FIPS + ", " + SCC +
            // "))";
            tempSelectDistinct = " SELECT DISTINCT e." + FIPS_COL_ORL + " as " + FIPS + ", e." + SCC_COL_ORL + " as "
                    + SCC + ", ";
            tempFromSelectDistinct = " FROM (SELECT DISTINCT " + FIPS_COL_ORL + ", " + SCC_COL_ORL + " FROM "
                    + qualifiedOrlTable + " ) e ";

            if (datasetType.equals(orlTypes.nonPoint())) {
                summarySelectDistinct = " SELECT DISTINCT f." + STATE_COL + " as " + STATE + ", " + "e." + FIPS_COL_ORL
                        + " as " + FIPS + ", e." + SCC_COL_ORL + " as " + SCC + ", e." + SIC_COL_ORL + " as " + SIC
                        + ", e." + MACT_COL_ORL + " as " + MACT + ", e." + SRCTYPE_COL + " as " + SRCTYPE + ", e."
                        + NAICS_COL_ORL + " as " + NAICS + ", ";
                summaryFromSelectDistinct += "(SELECT DISTINCT " + FIPS_COL_ORL + ", " + SCC_COL_ORL + ", "
                        + SIC_COL_ORL + ", " + MACT_COL_ORL + ", " + SRCTYPE_COL + ", " + NAICS_COL_ORL + " FROM "
                        + qualifiedOrlTable + " ) e ";
            } else {
                summarySelectDistinct = " SELECT DISTINCT f." + STATE_COL + " as " + STATE + ", " + "e." + FIPS_COL_ORL
                        + " as " + FIPS + ", e." + SCC_COL_ORL + " as " + SCC + ", ";
                summaryFromSelectDistinct += "(SELECT DISTINCT " + FIPS_COL_ORL + ", " + SCC_COL_ORL + " FROM "
                        + qualifiedOrlTable + " ) e ";
            }
        } else if (datasetType.equals(types.point())) {
            // String createIndex = " (INDEX orl_key (" + FIPS + ", " + PLANTID
            // + ", " + POINTID + ", "
            // + STACKID + ", " + SEGMENT + ", " + SCC + "))";
            summarySelectDistinct = " SELECT DISTINCT f." + STATE_COL + " as " + STATE + ", " + "e." + FIPS_COL_ORL
                    + " as " + FIPS + ", e." + PLANTID_COL + " as " + PLANTID + ", e." + POINTID_COL + " as " + POINTID
                    + ", e." + STACKID_COL + " as " + STACKID + ", e." + SEGMENT_COL + " as " + SEGMENT + ", e."
                    + SCC_COL_ORL + " as " + SCC + ", e." + PLANT_COL + " as " + PLANT + ", e." + ERPTYPE_COL + " as "
                    + ERPTYPE + ", e." + SRCTYPE_COL + " as " + SRCTYPE + ", e." + HEIGHT_COL_ORL + " as " + HEIGHT
                    + ", e." + DIAMETER_COL_ORL + " as " + DIAMETER + ", e." + EXIT_TEMP_COL_ORL + " as " + EXIT_TEMP
                    + ", e." + EXIT_FLOW_RATE_COL_ORL + " as " + EXIT_FLOW_RATE + ", e." + EXIT_VEL_COL_ORL + " as "
                    + EXIT_VEL + ", e." + SIC_COL_ORL + " as " + SIC + ", e." + MACT_COL_ORL + " as " + MACT + ", e."
                    + NAICS_COL_ORL + " as " + NAICS + ", e." + CTYPE_COL + " as " + CTYPE + ", e." + X_COORD_COL_ORL
                    + " as " + X_COORD + ", e." + Y_COORD_COL_ORL + " as " + Y_COORD + ", e." + UMTZ_COL + " as "
                    + UMTZ + ", ";
            summaryFromSelectDistinct += "(SELECT DISTINCT " + FIPS_COL_ORL + ", " + PLANTID_COL + ", " + POINTID_COL
                    + ", " + STACKID_COL + ", " + SEGMENT_COL + ", " + SCC_COL_ORL + ", " + PLANT_COL + ", "
                    + ERPTYPE_COL + ", " + SRCTYPE_COL + ", " + HEIGHT_COL_ORL + ", " + DIAMETER_COL_ORL + ", "
                    + EXIT_TEMP_COL_ORL + ", " + EXIT_FLOW_RATE_COL_ORL + ", " + EXIT_VEL_COL_ORL + ", " + SIC_COL_ORL
                    + ", " + MACT_COL_ORL + ", " + NAICS_COL_ORL + ", " + CTYPE_COL + ", " + X_COORD_COL_ORL + ", "
                    + Y_COORD_COL_ORL + ", " + UMTZ_COL + " FROM " + qualifiedOrlTable + " ) e ";
            tempSelectDistinct = " SELECT DISTINCT e." + FIPS_COL_ORL + " as " + FIPS + ", e." + PLANTID_COL + " as "
                    + PLANTID + ", e." + POINTID_COL + " as " + POINTID + ", e." + STACKID_COL + " as " + STACKID
                    + ", e." + SEGMENT_COL + " as " + SEGMENT + ", e." + SCC_COL_ORL + " as " + SCC + ", ";
            tempFromSelectDistinct = " FROM (SELECT DISTINCT " + FIPS_COL_ORL + ", " + PLANTID_COL + ", " + POINTID_COL
                    + ", " + STACKID_COL + ", " + SEGMENT_COL + ", " + SCC_COL_ORL + " FROM " + qualifiedOrlTable
                    + " ) e ";
        }

        // if there is a large number of tables, join small
        // groups to form subsets, then join the subsets
        final int MAX_POLLUTANTS_JOIN = 50;
        String[] tempTableNames = null;
        String[] tempTableQueries = null;
        List tempTableIndexList = new ArrayList();

        // these variables depend on whether or not we have subsets
        String summaryTableJoinPart = "";
        String summaryTableSelectPart = "";
        String summaryTableAndPart = "";

        // split into subgroups and use temp tables
        TableDefinition tableDefinition = this.emissionsDatasource.tableDefinition();
        if (numOfPollutants > MAX_POLLUTANTS_JOIN) {
            tempTableNames = new String[(int) Math.ceil(numOfPollutants / (double) MAX_POLLUTANTS_JOIN)];
            tempTableQueries = new String[tempTableNames.length];

            // get the names of the temp tables
            java.util.List tableNames = tableDefinition.getTableNames();
            for (int i = 0, nextTemp = 0; i < tempTableNames.length; i++, nextTemp++) {
                String tempTableName = summaryTable + "_temp" + nextTemp;
                while (tableNames.contains(tempTableName.toLowerCase())) {
                    nextTemp++;
                    tempTableName = summaryTable + "_temp" + nextTemp;
                }
                tempTableNames[i] = tempTableName;
            }

            // the select and join portions of the temporary table CREATE
            // statements
            String[] tempTableSelectParts = new String[tempTableNames.length];
            String[] tempTableJoinParts = new String[tempTableNames.length];
            java.util.Arrays.fill(tempTableSelectParts, "");
            java.util.Arrays.fill(tempTableJoinParts, "");
            for (int index = 0, i = 0; index < pollutants.length; index++, i = (index / MAX_POLLUTANTS_JOIN)) {
                // when using number (CAS) as column name, suffix w/ underscore
                // '_'
                String cleanPoll = "_" + pollutants[index].replace('-', '_');
                tempTableSelectParts[i] += cleanPoll + "." + emissionCol + " as " + cleanPoll + ", ";
                summaryTableSelectPart += "t" + i + "." + cleanPoll + " as " + cleanPoll + ", ";

                // get FIPS, SCC and CAS for pollutant
                if (datasetType.equals(orlTypes.nonPoint()) || datasetType.equals(types.nonRoad())
                        || datasetType.equals(types.onRoad())) {
                    tempTableJoinParts[i] += "LEFT JOIN (SELECT " + FIPS_COL_ORL + ", " + SCC_COL_ORL + ", "
                            + emissionCol + " FROM " + qualifiedOrlTable + " WHERE " + CAS_COL + " = '"
                            + pollutants[index] + "') " + cleanPoll + " ON (e." + FIPS_COL_ORL + " = " + cleanPoll
                            + "." + FIPS_COL_ORL + " AND e." + SCC_COL_ORL + " = " + cleanPoll + "." + SCC_COL_ORL
                            + ") ";
                    summaryTableAndPart += "e." + FIPS_COL_ORL + " = t" + i + "." + FIPS + " AND e." + SCC_COL_ORL
                            + " = t" + i + "." + SCC + " AND ";
                }
                // get FIPS, PLANTID, POINTID, STACKID, SEGMENT, SCC and CAS for
                // pollutant
                else if (datasetType.equals(types.point())) {
                    tempTableJoinParts[i] = tempTableJoinParts[i] + "LEFT JOIN (SELECT " + FIPS_COL_ORL + ", "
                            + PLANTID_COL + ", " + POINTID_COL + ", " + STACKID_COL + ", " + SEGMENT_COL + ", "
                            + SCC_COL_ORL + ", " + emissionCol + " FROM " + qualifiedOrlTable + " WHERE " + CAS_COL
                            + " = '" + pollutants[index] + "') " + cleanPoll + " ON (e." + FIPS_COL_ORL + " = "
                            + cleanPoll + "." + FIPS_COL_ORL + " AND e." + PLANTID_COL + " = " + cleanPoll + "."
                            + PLANTID_COL + " AND e." + POINTID_COL + " = " + cleanPoll + "." + POINTID_COL + " AND e."
                            + STACKID_COL + " = " + cleanPoll + "." + STACKID_COL + " AND e." + SEGMENT_COL + " = "
                            + cleanPoll + "." + SEGMENT_COL + " AND e." + SCC_COL_ORL + " = " + cleanPoll + "."
                            + SCC_COL_ORL + ") ";
                    summaryTableAndPart += "e." + FIPS_COL_ORL + " = t" + i + "." + FIPS + " AND e." + PLANTID_COL
                            + " = t" + i + "." + PLANTID + " AND e." + POINTID_COL + " = t" + i + "." + POINTID
                            + " AND e." + STACKID_COL + " = t" + i + "." + STACKID + " AND e." + SEGMENT_COL + " = t"
                            + i + "." + SEGMENT + " AND e." + SCC_COL_ORL + " = t" + i + "." + SCC + " AND ";
                }
            }

            // the temporary table CREATE statements

            for (int i = 0; i < tempTableNames.length; i++) {
                tempTableSelectParts[i] = tempTableSelectParts[i].substring(0, tempTableSelectParts[i].length() - 2);
                summaryTableJoinPart += tempTableNames[i] + " as t" + i + ", ";

                // TODO: create index after table creation
                tempTableQueries[i] = "CREATE TABLE " + tempTableNames[i] + " AS " + tempSelectDistinct
                        + tempTableSelectParts[i] + tempFromSelectDistinct + tempTableJoinParts[i];

                // schema.table -> schema_table
                String indexPrefix = tempTableNames[i].replace('.', '_');
                tempTableIndexList.add("CREATE INDEX " + indexPrefix + "_orl_key ON " + tempTableNames[i] + "(" + FIPS
                        + ", " + SCC + ")");
            }
        }
        // don't need to create temp tables
        else {
            // for each pollutant CAS code
            for (int i = 0; i < numOfPollutants; i++) {
                // when using number (CAS) as column name, enclose in slanted
                // quotes
                String cleanPoll = "_" + clean(pollutants[i]);
                summaryTableSelectPart += cleanPoll + "." + emissionCol + " as " + cleanPoll + ", ";

                if (datasetType.equals(orlTypes.nonPoint()) || datasetType.equals(types.nonRoad())
                        || datasetType.equals(types.onRoad())) {
                    // get FIPS, SCC and CAS for pollutant
                    summaryTableJoinPart += "LEFT JOIN (SELECT " + FIPS_COL_ORL + ", " + SCC_COL_ORL + ", "
                            + emissionCol + " FROM " + qualifiedOrlTable + " WHERE " + CAS_COL + " = '" + pollutants[i]
                            + "') " + cleanPoll + " ON (e." + FIPS_COL_ORL + " = " + cleanPoll + "." + FIPS_COL_ORL
                            + " AND e." + SCC_COL_ORL + " = " + cleanPoll + "." + SCC_COL_ORL + ") ";
                } else if (datasetType.equals(types.point())) {
                    // get FIPS, PLANTID, POINTID, STACKID, SEGMENT, SCC and CAS
                    // for pollutant
                    summaryTableJoinPart += "LEFT JOIN (SELECT " + FIPS_COL_ORL + ", " + PLANTID_COL + ", "
                            + POINTID_COL + ", " + STACKID_COL + ", " + SEGMENT_COL + ", " + SCC_COL_ORL + ", "
                            + emissionCol + " FROM " + qualifiedOrlTable + " WHERE " + CAS_COL + " = '" + pollutants[i]
                            + "') " + cleanPoll + " ON (e." + FIPS_COL_ORL + " = " + cleanPoll + "." + FIPS_COL_ORL
                            + " AND e." + PLANTID_COL + " = " + cleanPoll + "." + PLANTID_COL + " AND e." + POINTID_COL
                            + " = " + cleanPoll + "." + POINTID_COL + " AND e." + STACKID_COL + " = " + cleanPoll + "."
                            + STACKID_COL + " AND e." + SEGMENT_COL + " = " + cleanPoll + "." + SEGMENT_COL + " AND e."
                            + SCC_COL_ORL + " = " + cleanPoll + "." + SCC_COL_ORL + ") ";
                }
            }
        }

        // FIXME: drop all the tables before creating them
        // create the temp tables first, if needed
        if (tempTableNames != null) {
            for (int i = 0; i < tempTableQueries.length; i++) {
                emissionsQuery.execute(tempTableQueries[i]);
                emissionsQuery.execute((String) tempTableIndexList.get(i));
            }
            // will need comma after select distinct
            summaryFromSelectDistinct += ", ";
            // won't need comma after last table join
            summaryTableJoinPart = summaryTableJoinPart.substring(0, summaryTableJoinPart.length() - 2);
        }
        // the summary table CREATE statement
        summaryTableSelectPart = summaryTableSelectPart.substring(0, summaryTableSelectPart.length() - 2);
        String query = "CREATE TABLE " + qualifiedSummaryTable + /* createIndex + */" AS " + summarySelectDistinct
                + summaryTableSelectPart + summaryFromSelectDistinct + summaryTableJoinPart + " WHERE (e."
                + FIPS_COL_ORL + " = f." + FIPS_COL_REF + " AND " + summaryTableAndPart + "f.country_code='US')";

        // create the actual table
        if (tableDefinition.getTableNames().contains(qualifiedSummaryTable))
            throw new Exception("The table \"" + qualifiedSummaryTable
                    + "\" already exists. Please select 'overwrite tables if exist' or choose a new table name.");
        emissionsQuery.execute(query);

        // drop the temp tables, if needed
        tableDefinition.deleteTable(qualifiedOrlTable);
        if (tempTableNames != null) {
            for (int i = 0; i < tempTableNames.length; i++) {
                tableDefinition.deleteTable(tempTableNames[i]);
            }
        }

    }

}
