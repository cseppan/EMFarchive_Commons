package gov.epa.emissions.commons.io.importer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

//FIXME: what's this mess ?
public class ORLTableTypes {

    private static final ORLDatasetTypesFactory datasetTypes = new DefaultORLDatasetTypesFactory();

    public static final ORLTableType ORL_AREA_NONPOINT_TOXICS = new ORLTableType(datasetTypes.nonPoint());

    public static final ORLTableType ORL_AREA_NONROAD_TOXICS = new ORLTableType(datasetTypes.nonRoad());

    public static final ORLTableType ORL_ONROAD_MOBILE_TOXICS = new ORLTableType(datasetTypes.onRoad());

    public static final ORLTableType ORL_POINT_TOXICS = new ORLTableType(datasetTypes.point());

    private List list() {
        List list = new ArrayList();

        list.add(ORL_AREA_NONPOINT_TOXICS);
        list.add(ORL_AREA_NONROAD_TOXICS);
        list.add(ORL_ONROAD_MOBILE_TOXICS);
        list.add(ORL_POINT_TOXICS);

        return list;
    }

    public ORLTableType type(String datasetType) {
        for (Iterator iter = list().iterator(); iter.hasNext();) {
            ORLTableType type = (ORLTableType) iter.next();
            if (type.datasetType().getName().equals(datasetType))
                return type;
        }

        return null;
    }

}
