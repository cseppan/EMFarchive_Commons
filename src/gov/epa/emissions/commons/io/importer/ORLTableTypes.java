package gov.epa.emissions.commons.io.importer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

//FIXME: what's this mess ?
public class ORLTableTypes implements TableTypes {

    private static final ORLDatasetTypesFactory datasetTypes = new DefaultORLDatasetTypesFactory();

    public static final TableType ORL_AREA_NONPOINT_TOXICS = new TableType(datasetTypes.nonPoint(),
            new String[] { "ORL Nonpoint Inventory" }, "ORL Nonpoint Inventory Summary");

    public static final TableType ORL_AREA_NONROAD_TOXICS = new TableType(datasetTypes.nonRoad(),
            new String[] { "ORL Nonroad Inventory" }, "ORL Nonroad Inventory Summary");

    public static final TableType ORL_ONROAD_MOBILE_TOXICS = new TableType(datasetTypes.onRoad(),
            new String[] { "ORL Onroad Inventory" }, "ORL Onroad Inventory Summary");

    public static final TableType ORL_POINT_TOXICS = new TableType(datasetTypes.point(),
            new String[] { "ORL Point Inventory" }, "ORL Point Inventory Summary");

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
            if (type.datasetType().getName().equals(datasetType))
                return type;
        }

        return null;
    }

}
