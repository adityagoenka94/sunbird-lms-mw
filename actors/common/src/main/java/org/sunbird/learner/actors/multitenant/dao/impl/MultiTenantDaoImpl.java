package org.sunbird.learner.actors.multitenant.dao.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.CaminoJsonKey;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.multitenant.dao.MultiTenantDao;
import org.sunbird.learner.util.CaminoUtil;
import org.sunbird.learner.util.Util;
import org.sunbird.models.multitenant.MultiTenant;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MultiTenantDaoImpl implements MultiTenantDao{
    private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
   private Util.DbInfo tenantInfoDb = CaminoUtil.dbInfoMap.get(CaminoJsonKey.MULTI_TENANT_INFO_DB);


    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public Response createMultiTenantInfo(MultiTenant multiTenant) {
        Map<String, Object> map = mapper.convertValue(multiTenant, Map.class);

        // Insert record in tenant_info table of cassandra database for Camino Instance
        return cassandraOperation.insertRecord(
                tenantInfoDb.getKeySpace(), tenantInfoDb.getTableName(), map);
    }


    @Override
    public Response readMultiTenantInfoByProperty(String property,String propertyValue) {


        // Get record from tenant_info table of cassandra database for Camino Instance

        Response tenantInfoResult =
                cassandraOperation.getRecordsByProperty(
                        tenantInfoDb.getKeySpace(), tenantInfoDb.getTableName(), property, propertyValue);
            return tenantInfoResult;
    }


    @Override
    public MultiTenant readMultiTenantInfoById(String id) {

        Response tenantPreferenceDetail =
                cassandraOperation.getRecordById(
                        tenantInfoDb.getKeySpace(), tenantInfoDb.getTableName(), id);
        List<Map<String, Object>> tenantPreference =
                (List<Map<String, Object>>) tenantPreferenceDetail.get(JsonKey.RESPONSE);
        if ((tenantPreference.isEmpty())) {
            throw new ProjectCommonException(
                    ResponseCode.invalidTenantInfoId.getErrorCode(),
                    ResponseCode.invalidTenantInfoId.getErrorMessage(),
                    ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
        } else {
            return mapper.convertValue(tenantPreference.get(0), MultiTenant.class);
        }
    }

    @Override
    public Response updateMultiTenantInfo(MultiTenant multiTenant) {

        Map<String, Object> map = mapper.convertValue(multiTenant, Map.class);

        // update record in multi_tenant_info table of cassandra database for Camino Instance
        return cassandraOperation.updateRecord(
                tenantInfoDb.getKeySpace(), tenantInfoDb.getTableName(), map);
    }


    @Override
    public Response deleteMultiTenantInfo(String multiTenantId) {

        return cassandraOperation.deleteRecord(
                tenantInfoDb.getKeySpace(), tenantInfoDb.getTableName(),multiTenantId);
    }


}
