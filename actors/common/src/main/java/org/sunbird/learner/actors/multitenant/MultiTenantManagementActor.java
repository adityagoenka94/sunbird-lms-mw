package org.sunbird.learner.actors.multitenant;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.*;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.dto.SearchDTO;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.CaminoUtil;
import org.sunbird.learner.util.Util;
import org.sunbird.learner.actors.multitenant.dao.MultiTenantDao;
import org.sunbird.common.models.util.ProjectUtil.EsIndex;
import org.sunbird.learner.actors.multitenant.dao.impl.MultiTenantDaoImpl;
import org.sunbird.models.multitenant.MultiTenant;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

@ActorConfig(
        tasks = {
                "createMultiTenantInfo",
                "updateMultiTenantInfo",
                "getMultiTenantInfo",
                "deleteMultiTenantInfo"
        },
        asyncTasks = {}
)

public class MultiTenantManagementActor extends BaseActor {

    // private MultiTenantService multiTenantService = new MultiTenantService();
    ObjectMapper mapper = new ObjectMapper();
    private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
    private Util.DbInfo tenantInfoDb = CaminoUtil.dbInfoMap.get(CaminoJsonKey.MULTI_TENANT_INFO_DB);
    private Util.DbInfo orgDb = Util.dbInfoMap.get(JsonKey.ORG_DB);
    private MultiTenantDao multiTenantDao = new MultiTenantDaoImpl();

    @Override
    public void onReceive(Request request) throws Throwable {

        Util.initializeContext(request, CaminoTelemetryEnvKey.TENANT_INFO);
        ExecutionContext.setRequestId(request.getRequestId());

        String requestedOperation = request.getOperation();
        switch (requestedOperation) {
            case "createMultiTenantInfo":
                createMultiTenantInfo(request);
                break;
            case "updateMultiTenantInfo":
                updateMultiTenantInfo(request);
                break;
            case "getMultiTenantInfo":
                getMultiTenantInfo(request);
                break;
            case "deleteMultiTenantInfo":
                deleteMultiTenantInfo(request);
                break;
            default:
                onReceiveUnsupportedOperation(request.getOperation());
                break;
        }
    }

    // To create Multi Tenant Info
    private void createMultiTenantInfo(Request actorMessage) {

        ProjectLogger.log("Create Multi Tenant Info Api Called");
        Map<String, Object> request = actorMessage.getRequest();
        String requestedBy = (String) actorMessage.getContext().get(JsonKey.REQUESTED_BY);
        String tenantInfoId = ProjectUtil.getUniqueIdFromTimestamp(actorMessage.getEnv());
        Map<String, Object> tenantPreferenceData=null;
        String jsonString;

        MultiTenant multiTenant;
        String homeUrl = (String) request.get(JsonKey.HOME_URL);

        validateHomeUrl(homeUrl);


        String[] search = new String[1];
        search[0] = "org";
        Map<String, Object> searchQueryMap = new LinkedHashMap<>();
        LinkedHashMap<String, String> data = new LinkedHashMap<>();
        data.put(JsonKey.HOME_URL, homeUrl);
        searchQueryMap.put("filters", data);
        SearchDTO searchDto = Util.createSearchDto(searchQueryMap);
        Map<String, Object> result =
                ElasticSearchUtil.complexSearch(
                        searchDto,
                        EsIndex.sunbird.getIndexName(),
                        search);

        Map<String, Object> orgData = validateOrgSearchResult(result);

        multiTenant = mapper.convertValue(orgData, MultiTenant.class);

        // Case if TenantPreferenceDetails is Empty, then apply default settings
        if (StringUtils.isBlank(request.get(CaminoJsonKey.PREFERENCE_DETAILS).toString())) {
            try {
                ProjectLogger.log(
                        "MultiTenantManagementActor:createTenantPreferenceDetails():  Applying default tenant preference settings.",
                        LoggerEnum.INFO.name());
                InputStream file = this.getClass().getResourceAsStream( "/data/defaultTenantPreferenceData.json" );
                byte[] defaultData = new byte[file.available()];
                file.read(defaultData);
                jsonString = new String(defaultData, StandardCharsets.UTF_8);
                ObjectMapper mapper = new ObjectMapper();
                JsonNode actualObj = mapper.readTree(jsonString);
                tenantPreferenceData=mapper.convertValue(actualObj,Map.class);
                file.close();
            } catch (IOException e) {
                ProjectLogger.log(
                        "MultiTenantManagementActor:createTenantPreferenceDetails():  Error while applying default tenant preference settings.",
                        LoggerEnum.ERROR.name());
                e.printStackTrace();
                throw new ProjectCommonException(
                        ResponseCode.valueSyntaxError.getErrorCode(),
                        ResponseCode.valueSyntaxError.getErrorMessage(),
                        ResponseCode.CLIENT_ERROR.getResponseCode());
            }
        }
        // If TenantPreferenceDetails is Not Empty
        else {
            tenantPreferenceData=(LinkedHashMap<String, Object>) request.get(CaminoJsonKey.PREFERENCE_DETAILS);
        }

        storeTenantPreferenceData(tenantPreferenceData, multiTenant);

        if (!StringUtils.isBlank((String)request.get(JsonKey.FRAMEWORK))) {
            multiTenant.setFramework((String)request.get(JsonKey.FRAMEWORK));
        }
        else {
            multiTenant.setFramework("NCF");
        }


        multiTenant.setId(tenantInfoId);
        multiTenant.setOrgId((String)orgData.get(JsonKey.ID));
        multiTenant.setMultiTenantId(tenantInfoId);
        multiTenant.setCreatedBy(requestedBy);
        multiTenant.setCreatedDate(ProjectUtil.getFormattedDate());

            Response response = multiTenantDao.createMultiTenantInfo(multiTenant);
        response.put(CaminoJsonKey.MULTI_TENANT_ID, tenantInfoId);
        ProjectLogger.log(
                "MultiTenantManagementActor:createTenant():  Tenant info created successfully "+response,
                LoggerEnum.INFO.name());

        sender().tell(response, self());
    }


    // To update Tenant Info
    private void updateMultiTenantInfo(Request actorMessage) {

        ProjectLogger.log("Update Tenant Info Api called");
        Map<String, Object> request = actorMessage.getRequest();
        String requestedBy = (String) actorMessage.getContext().get(JsonKey.REQUESTED_BY);

        String data=null;
        Response result=null;
        MultiTenant multiTenant = null;
        if(request.get(CaminoJsonKey.MULTI_TENANT_ID)!=null) {
            data = (String) request.get(CaminoJsonKey.MULTI_TENANT_ID);
            multiTenant = multiTenantDao.readMultiTenantInfoById(data);
        }
        else {
            data = (String) request.get(JsonKey.HOME_URL);
            result = multiTenantDao.readMultiTenantInfoByProperty(JsonKey.HOME_URL, data);
            List<Map<String, Object>> tenantList =
                    (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
            if (tenantList.isEmpty()) {
                ProjectLogger.log(
                        "MultiTenantManagementActor:updateMultiTenantInfo():  No Tenant exists with this Home Url",
                        LoggerEnum.ERROR.name());
                throw new ProjectCommonException(
                        ResponseCode.invalidHomeUrl.getErrorCode(),
                        ResponseCode.invalidHomeUrl.getErrorMessage(),
                        ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
            }
            multiTenant = mapper.convertValue(tenantList.get(0),MultiTenant.class);
        }

        String orgId=multiTenant.getOrgId();
        MultiTenant newMultiTenant = null;
        Response response =
                cassandraOperation.getRecordById(orgDb.getKeySpace(), orgDb.getTableName(),orgId);
        List<Map<String, Object>> tenantInfoList =
                (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
        if(tenantInfoList.isEmpty())
        {
            ProjectLogger.log(
                    "MultiTenantManagementActor:updateTenantInfo():  Organisation does not exists.",
                    LoggerEnum.ERROR.name());
            throw new ProjectCommonException(
                    ResponseCode.invalidRootOrganisationId.getErrorCode(),
                    ResponseCode.invalidRootOrganisationId.getErrorMessage()+" : orgId = "+orgId,
                    ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
        }
        newMultiTenant = mapper.convertValue(tenantInfoList.get(0), MultiTenant.class);

        if(!StringUtils.isBlank((String)request.get(JsonKey.FRAMEWORK)))
        {
            newMultiTenant.setFramework((String)request.get(JsonKey.FRAMEWORK));
        }

        if( request.get(CaminoJsonKey.PREFERENCE_DETAILS)!=null && StringUtils.isBlank(request.get(CaminoJsonKey.PREFERENCE_DETAILS).toString()))
        {
            Map<String, Object> tenantPreferenceData=(LinkedHashMap<String, Object>) request.get(CaminoJsonKey.PREFERENCE_DETAILS);
            storeTenantPreferenceData(tenantPreferenceData, newMultiTenant);
        }

        newMultiTenant.setId(multiTenant.getId());
        newMultiTenant.setOrgId(multiTenant.getOrgId());
        newMultiTenant.setMultiTenantId(multiTenant.getMultiTenantId());
        newMultiTenant.setCreatedBy(multiTenant.getCreatedBy());
        newMultiTenant.setCreatedDate(multiTenant.getCreatedDate());
        newMultiTenant.setUpdatedBy(requestedBy);
        newMultiTenant.setUpdatedDate(ProjectUtil.getFormattedDate());

            response = multiTenantDao.updateMultiTenantInfo(newMultiTenant);
            response.put(CaminoJsonKey.MULTI_TENANT_ID,newMultiTenant.getMultiTenantId());

        ProjectLogger.log(
                "MultiTenantManagementActor:updateTenantInfo():  Tenant Info updated successfully. ",
                LoggerEnum.INFO.name());
        sender().tell(response, self());
    }


    // To get Tenant Info
    private void getMultiTenantInfo(Request actorMessage) {

        ProjectLogger.log("Get Tenant Info Api called");
        Map<String, Object> request = actorMessage.getRequest();
        String data=null;
        Response result=null;
        if(request.get(JsonKey.HOME_URL)!=null) {
            data = (String) request.get(JsonKey.HOME_URL);
            result = multiTenantDao.readMultiTenantInfoByProperty(JsonKey.HOME_URL, data);
        }
        else {
            data = (String) request.get(JsonKey.ORGANISATION_ID);
            result = multiTenantDao.readMultiTenantInfoByProperty(JsonKey.ORG_ID, data);

        }

        List<Map<String, Object>> tenantList =
                (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
        String newHomeUrl = null;


        if (tenantList.isEmpty()) {

            // if Tenant Info is not found, change the url for rootOrg
            // example : RootOrg Url - www.sunbird.com
            // subOrg Url - www.sunbird.com/subOrg1
            // another subOrg url - www.sunbird.com/subOrg2
            // So remove the subOrg part from url and return the details of the rootOrg

            if (request.get(JsonKey.HOME_URL)!=null) {
                newHomeUrl = data.substring(0, data.lastIndexOf('/'));
                result = multiTenantDao.readMultiTenantInfoByProperty(JsonKey.HOME_URL, newHomeUrl);
            }

            // if Tenant Info is not found, get the rootOrgId of that organisation
            else if(request.get(JsonKey.ORGANISATION_ID)!=null) {
                Map<String, Object> orgDetails =
                        ElasticSearchUtil.getDataByIdentifier(
                                ProjectUtil.EsIndex.sunbird.getIndexName(),
                                ProjectUtil.EsType.organisation.getTypeName(),
                                data);

                if (MapUtils.isEmpty(orgDetails)) {
                    ProjectLogger.log(
                            "MultiTenantManagementActor:getMultiTenantInfo():  No organisation exists with this id.",
                            LoggerEnum.ERROR.name());
                    throw new ProjectCommonException(
                            ResponseCode.orgDoesNotExist.getErrorCode(),
                            ResponseCode.orgDoesNotExist.getErrorMessage(),
                            ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
                }

                String rootOrgId = (String) orgDetails.get(JsonKey.ROOT_ORG_ID);
                result = multiTenantDao.readMultiTenantInfoByProperty(JsonKey.ORG_ID, rootOrgId);
            }

            tenantList =
                    (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
        }

        if (tenantList.isEmpty()) {
            ProjectLogger.log(
                    "MultiTenantManagementActor:getMultiTenantInfo():  No Tenant exists with this data",
                    LoggerEnum.ERROR.name());
            throw new ProjectCommonException(
                    ResponseCode.invalidData.getErrorCode(),
                    ResponseCode.invalidData.getErrorMessage(),
                    ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
        }
        String preferenceDetails = (String)(tenantList.get(0).get(CaminoJsonKey.PREFERENCE_DETAILS));
        JsonNode jsonNode = null;

        try {
            jsonNode = mapper.readTree(preferenceDetails);
        }
         catch (IOException e) {
            ProjectLogger.log(
                    "MultiTenantManagementActor:getMultiTenantInfo():  Error while applying default tenant preference settings.",
                    LoggerEnum.ERROR.name());
            e.printStackTrace();
            throw new ProjectCommonException(
                    ResponseCode.valueSyntaxError.getErrorCode(),
                    ResponseCode.valueSyntaxError.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }
        Map<String,Object> map = mapper.convertValue(jsonNode,Map.class);

        tenantList.get(0).put(CaminoJsonKey.PREFERENCE_DETAILS,map);
        ProjectLogger.log(
                "MultiTenantManagementActor:getMultiTenantInfo():  Tenant Info = "+result,
                LoggerEnum.INFO.name());
        sender().tell(result, self());
    }


    public void deleteMultiTenantInfo(Request actorMessage) {

        ProjectLogger.log("Delete Multi Tenant Info Api Called");
        Map<String, Object> request = actorMessage.getRequest();
        String multiTenantId = (String)request.get(CaminoJsonKey.MULTI_TENANT_ID);

        if (StringUtils.isBlank(multiTenantId)) {
            ProjectLogger.log(
                    "MultiTenantManagementActor:deleteMultiTenantInfo():  Please provide a valid data.",
                    LoggerEnum.ERROR.name());
            throw new ProjectCommonException(
                    ResponseCode.invalidRequestData.getErrorCode(),
                    ResponseCode.invalidRequestData.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }

        Response response = multiTenantDao.deleteMultiTenantInfo(multiTenantId);
        sender().tell(response, self());
    }



    /**
     *Returns Organisation details according to the input(result) as described below
     *
     * @param result contains only one organisation details,
     * then its HomeUrl is unique and
     * @return Response containing Organisation Details
     *
     * @param result contains multiple organisation details,
     * then it finds the root organisation and
     * @return Response containing Organisation Details
     */
    private Map<String, Object> validateOrgSearchResult(Map<String, Object> result) {

        Map<String, Object> orgDetails=null;
        List<Map<String, Object>> listRootOrg = new ArrayList<>();
        long count = (long) result.get(JsonKey.COUNT);
        ProjectLogger.log(
                "MultiTenantManagementActor:validateOrgSearchResult():  Organisations with the homeUrl count = " + count,
                LoggerEnum.INFO.name());
        if(count == 1){
            ProjectLogger.log(
                    "MultiTenantManagementActor:validateOrgSearchResult():  Unique Organisation found with the homeUrl.",
                    LoggerEnum.INFO.name());
            List<Object> content = (ArrayList<Object>) result.get(JsonKey.CONTENT);
            orgDetails = (HashMap<String, Object>) content.get(0);
            return orgDetails;
        }

        else if (count > 1) {
            ProjectLogger.log(
                    "MultiTenantManagementActor:validateOrgSearchResult():  Multiple Organisations found with the homeUrl. Selecting RootOrg. ",
                    LoggerEnum.INFO.name());
            List<Object> content = (ArrayList<Object>) result.get(JsonKey.CONTENT);
            Iterator iterator = content.iterator();
            while(iterator.hasNext()){
                orgDetails = (HashMap<String, Object>)iterator.next();
                boolean check=(boolean)orgDetails.get(JsonKey.IS_ROOT_ORG);
                if(check)
                    listRootOrg.add(orgDetails);
            }
            if(listRootOrg.size()==1)
            return listRootOrg.get(0);
            else {
                ProjectLogger.log(
                        "MultiTenantManagementActor:createTenant():  Multiple Root Organisation found with the homeUrl.",
                        LoggerEnum.INFO.name());
                throw new ProjectCommonException(
                        ResponseCode.multipleRootOrgsWithSameHomeUrl.getErrorCode(),
                        ResponseCode.multipleRootOrgsWithSameHomeUrl.getErrorMessage(),
                        ResponseCode.CLIENT_ERROR.getResponseCode());
            }
        }
        else
        {
            ProjectLogger.log(
                    "MultiTenantManagementActor:createTenant():  No Organisation found with the homeUrl.",
                    LoggerEnum.INFO.name());
            throw new ProjectCommonException(
                    ResponseCode.invalidHomeUrl.getErrorCode(),
                    ResponseCode.invalidHomeUrl.getErrorMessage(),
                    ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
        }
    }


/**
 * Checks if homeUrl already exists in tenant_info table
 * If exists, throws Error
 */
    private void validateHomeUrl(String homeUrl){

        Map<String,Object> url=new HashMap<>();
        url.put(JsonKey.HOME_URL,homeUrl);

        // Get Tenant Info using homeUrl
        Response tenantInfoResult =
                multiTenantDao.readMultiTenantInfoByProperty(JsonKey.HOME_URL, homeUrl);
        List<Map<String, Object>> tenantList =
                (List<Map<String, Object>>) tenantInfoResult.get(JsonKey.RESPONSE);
        if (!tenantList.isEmpty()) {
            ProjectLogger.log(
                    "MultiTenantManagementActor:validateHomeUrl():  Organisation with url = " + homeUrl+ " already exists in tenant_info table",
                    LoggerEnum.ERROR.name());
            throw new ProjectCommonException(
                    ResponseCode.homeUrlAlreadyExists.getErrorCode(),
                    ResponseCode.homeUrlAlreadyExists.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }
    }

    private void storeTenantPreferenceData(Map<String,Object> tenantPreferenceData,MultiTenant multiTenant) {

        JsonNode tenantDetails = mapper.convertValue(tenantPreferenceData,JsonNode.class);
        try {
            multiTenant.setPreferenceDetails(mapper.writeValueAsString(tenantDetails));
        }
        catch(JsonProcessingException e)
        {
            ProjectLogger.log(
                    "MultiTenantManagementActor:createTenantPreferenceDetails():  Data format error.",
                    LoggerEnum.ERROR.name());
            throw new ProjectCommonException(
                    ResponseCode.jsonDataFormatError.getErrorCode(),
                    ResponseCode.jsonDataFormatError.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }
    }

    }