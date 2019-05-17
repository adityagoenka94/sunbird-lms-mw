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
import org.sunbird.models.multitenant.TenantPreferenceDetails;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MultiTenantDaoImpl implements MultiTenantDao{
    private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
   private Util.DbInfo tenantInfoDb = CaminoUtil.dbInfoMap.get(CaminoJsonKey.TENANT_INFO_DB);
    private Util.DbInfo tenantPreferenceDetailsDb = CaminoUtil.dbInfoMap.get(CaminoJsonKey.TENANT_PREFERENCE_DETAILS_DB);



    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public Response createTenantInfo(MultiTenant multiTenant) {
        Map<String, Object> map = mapper.convertValue(multiTenant, Map.class);

        // Insert record in tenant_info table of cassandra database for Camino Instance
        return cassandraOperation.insertRecord(
                tenantInfoDb.getKeySpace(), tenantInfoDb.getTableName(), map);
    }

    @Override
    public Response createTenantPreferenceData(TenantPreferenceDetails tenantPreferenceData) {
        Map<String, Object> map = mapper.convertValue(tenantPreferenceData, Map.class);

        // Insert record in tenant_preference_details table of cassandra database for Camino Instance

        return cassandraOperation.insertRecord(
                tenantPreferenceDetailsDb.getKeySpace(), tenantPreferenceDetailsDb.getTableName(), map);
    }



    @Override
    public Response readTenantInfoByHomeUrl(String homeUrl) {
        Map<String,Object> url=new HashMap<>();

        url.put(JsonKey.HOME_URL,homeUrl);

        // Get record from tenant_info table of cassandra database for Camino Instance

        Response tenantInfoResult =
                cassandraOperation.getRecordsByProperties(
                        tenantInfoDb.getKeySpace(), tenantInfoDb.getTableName(), url);
            return tenantInfoResult;
    }

    @Override
    public Response readTenantInfoByOrgId(String orgid) {
        Map<String,Object> url=new HashMap<>();

        url.put(JsonKey.ORG_ID,orgid);

        // Get record from tenant_info table of cassandra database for Camino Instance

        Response tenantInfoResult =
                cassandraOperation.getRecordsByProperties(
                        tenantInfoDb.getKeySpace(), tenantInfoDb.getTableName(), url);
        return tenantInfoResult;
    }

    @Override
    public Response readTenantPreferenceDetailsByOrgId(String orgId) {

        Map<String,Object> orgDetail=new HashMap<>();
        orgDetail.put(JsonKey.ORG_ID,orgId);

        // Get record from tenant_preference_details table of cassandra database for Camino Instance

        Response tenantPreferenceDetails = cassandraOperation.getRecordsByProperties(
                tenantPreferenceDetailsDb.getKeySpace(), tenantPreferenceDetailsDb.getTableName(), orgDetail);
        List<Map<String, Object>> tenantPreferenceList =
                (List<Map<String, Object>>) tenantPreferenceDetails.get(JsonKey.RESPONSE);
        if ((tenantPreferenceList.isEmpty())) {
            throw new ProjectCommonException(
                    ResponseCode.invalidOrgId.getErrorCode(),
                    ResponseCode.invalidOrgId.getErrorMessage(),
                    ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
        } else {
            return tenantPreferenceDetails;
        }
    }


    @Override
    public TenantPreferenceDetails readTenantPreferenceDetailById(String id) {

        Response tenantPreferenceDetail =
                cassandraOperation.getRecordById(
                        tenantPreferenceDetailsDb.getKeySpace(), tenantPreferenceDetailsDb.getTableName(), id);
        List<Map<String, Object>> tenantPreference =
                (List<Map<String, Object>>) tenantPreferenceDetail.get(JsonKey.RESPONSE);
        if ((tenantPreference.isEmpty())) {
            throw new ProjectCommonException(
                    ResponseCode.invalidTenantPreferenceDetailId.getErrorCode(),
                    ResponseCode.invalidTenantPreferenceDetailId.getErrorMessage(),
                    ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
        } else {
            return mapper.convertValue(tenantPreference.get(0), TenantPreferenceDetails.class);
        }
    }


    @Override
    public MultiTenant readTenantInfoById(String id) {

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

}
