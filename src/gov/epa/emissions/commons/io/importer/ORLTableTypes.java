package gov.epa.emissions.commons.io.importer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

//FIXME: what's this mess ?
public class ORLTableTypes implements TableTypes {

    private static final ORLDatasetTypes datasetTypes = new ORLDatasetTypes();

    public static final TableType ORL_AREA_NONPOINT_TOXICS = new TableType(datasetTypes.nonPoint().getName(),
            new String[] { "ORL Nonpoint Inventory" }, "ORL Nonpoint Toxics Summary");

    public static final TableType ORL_AREA_NONROAD_TOXICS = new TableType(datasetTypes.nonRoad().getName(),
            new String[] { "ORL Nonroad Source Toxics" }, "ORL Nonroad Toxics Summary");

    public static final TableType ORL_ONROAD_MOBILE_TOXICS = new TableType(datasetTypes.onRoad().getName(),
            new String[] { "ORL Mobile Source Toxics" }, "ORL Mobile Toxics Summary");

    public static final TableType ORL_POINT_TOXICS = new TableType(datasetTypes.point().getName(),
            new String[] { "ORL Point Inventory" }, "ORL Point Toxics Summary");

    private List list() {
        List list = new ArrayList();

        list.add(ORL_AREA_NONPOINT_TOXICS);
        list.add(ORL_AREA_NONROAD_TOXICS);
        list.add(ORL_ONROAD_MOBILE_TOXICS);
        list.add(ORL_POINT_TOXICS);

        return list;
    }

    public TableType type(String datasetType) {
        for (Iterator iter = list().iterator(); iter.hasNext();) {
            TableType type = (TableType) iter.next();
            if (type.getDatasetType().equals(datasetType))
                return type;
        }

        return null;
    }

}
