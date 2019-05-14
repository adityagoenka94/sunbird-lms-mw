package org.sunbird.learner.util;

import org.sunbird.common.models.util.CaminoJsonKey;
import java.util.HashMap;
import java.util.Map;
import static org.sunbird.learner.util.Util.DbInfo;
import static org.sunbird.learner.util.Util.KEY_SPACE_NAME;

/** This class will initialize the cassandra data base property for Camino Instance*/

public class CaminoUtil {

    public static final Map<String, DbInfo> dbInfoMap = new HashMap<>(Util.dbInfoMap);

    private CaminoUtil(){
    }

    static {
        initializeDBProperty();
    }
    private static void initializeDBProperty() {

        dbInfoMap.put(CaminoJsonKey.TENANT_INFO_DB,getDbInfoObject (KEY_SPACE_NAME, "tenant_info"));
        dbInfoMap.put(CaminoJsonKey.TENANT_PREFERENCE_DETAILS_DB,getDbInfoObject(KEY_SPACE_NAME, "tenant_preference_details"));

    }

    private static DbInfo getDbInfoObject(String keySpace, String table) {

        DbInfo dbInfo = new DbInfo();

        dbInfo.setKeySpace(keySpace);
        dbInfo.setTableName(table);

        return dbInfo;
    }

}