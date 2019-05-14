package org.sunbird.learner.actors.multitenant;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.sunbird.models.multitenant.TenantPreferenceDetails;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

@ActorConfig(
        tasks = {
                "createTenant",
                "updateTenantInfo",
                "updateTenantPreferenceDetails",
                "getTenantInfo",
                "addTenantPreferenceDetails"
        },
        asyncTasks = {}
)

public class MultiTenantManagementActor extends BaseActor {

    // private MultiTenantService multiTenantService = new MultiTenantService();
    ObjectMapper mapper = new ObjectMapper();
    private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
    private Util.DbInfo tenantInfoDb = CaminoUtil.dbInfoMap.get(CaminoJsonKey.TENANT_INFO_DB);
    private Util.DbInfo tenantPreferenceDetailsDb = CaminoUtil.dbInfoMap.get(CaminoJsonKey.TENANT_PREFERENCE_DETAILS_DB);
    private Util.DbInfo orgDb = Util.dbInfoMap.get(JsonKey.ORG_DB);
    private MultiTenantDao multiTenantDao = new MultiTenantDaoImpl();

    @Override
    public void onReceive(Request request) throws Throwable {

        Util.initializeContext(request, CaminoTelemetryEnvKey.TENANT_INFO);
        ExecutionContext.setRequestId(request.getRequestId());

        String requestedOperation = request.getOperation();
        switch (requestedOperation) {
            case "createTenant":
                createTenantInfo(request);
                break;
            case "updateTenantInfo":
                updateTenantInfo(request);
                break;
            case "updateTenantPreferenceDetails":
                updateTenantPreferenceDetails(request);
                break;
            case "getTenantInfo":
                getTenantInfo(request);
                break;
            case "addTenantPreferenceDetails":
                addTenantPreferenceDetails(request);
                break;
            default:
                onReceiveUnsupportedOperation(request.getOperation());
                break;
        }
    }

    // To create Tenant Info
    private void createTenantInfo(Request actorMessage) {

        Map<String, Object> request = actorMessage.getRequest();
        String requestedBy = (String) actorMessage.getContext().get(JsonKey.REQUESTED_BY);
        String tenantInfoId = ProjectUtil.getUniqueIdFromTimestamp(actorMessage.getEnv());

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
        multiTenant.setId(tenantInfoId);
        multiTenant.setOrgId((String)orgData.get(JsonKey.ID));
        Response response = multiTenantDao.createTenantInfo(multiTenant);
        response.put(CaminoJsonKey.TENANT_INFO_ID, tenantInfoId);
        request.put(JsonKey.ORG_ID,orgData.get(JsonKey.ID));
        request.put(JsonKey.CREATED_BY,requestedBy);
        actorMessage.setRequest(request);
        createTenantPreferenceDetails(actorMessage);

        sender().tell(response, self());
    }

    // To create Tenant Preference Details
    private Response createTenantPreferenceDetails(Request actorMessage) {

        Map<String, Object> request = actorMessage.getRequest();
        String jsonString;
        Response result=null;
        String orgId=(String)request.get(JsonKey.ORG_ID);
        Map<String, Object> tenantPreferenceData=null;

        // Case if TenantPreferenceDetails is Empty, then apply default settings
        if (StringUtils.isBlank(request.get(CaminoJsonKey.TENANT_PREFERENCE_DETAILS).toString())) {
            try {
                InputStream file = this.getClass().getResourceAsStream( "/data/defaultTenantPreferenceData.json" );
                byte[] data = new byte[file.available()];
                file.read(data);
                jsonString = new String(data, StandardCharsets.UTF_8);
                ObjectMapper mapper = new ObjectMapper();
                JsonNode actualObj = mapper.readTree(jsonString);
                tenantPreferenceData=mapper.convertValue(actualObj,Map.class);
                file.close();
            } catch (IOException e) {
                e.printStackTrace();
                throw new ProjectCommonException(
                        ResponseCode.valueSyntaxError.getErrorCode(),
                        ResponseCode.valueSyntaxError.getErrorMessage(),
                        ResponseCode.CLIENT_ERROR.getResponseCode());
            }
        }
        // If TenantPreferenceDetails is Not Empty
        else {
            tenantPreferenceData=(LinkedHashMap<String, Object>) request.get(CaminoJsonKey.TENANT_PREFERENCE_DETAILS);
        }
        Map<String,Object> newRequest=new LinkedHashMap<>();

        newRequest.put(JsonKey.ORG_ID,orgId);
        for (Map.Entry<String, Object> entry : tenantPreferenceData.entrySet()) {
            String pageName = entry.getKey();
            newRequest.put(JsonKey.PAGE,pageName);
            Map<String, Object> pageDetails= (LinkedHashMap<String,Object>)entry.getValue();
            for (Map.Entry<String, Object> entry2 : pageDetails.entrySet()) {
                String keyName=entry2.getKey();
                newRequest.put(JsonKey.KEY,keyName);
                Map<String, Object> keyValueDetails=(LinkedHashMap<String,Object>)entry2.getValue();
                ObjectMapper objectMapper=new ObjectMapper();
                String keyValue=null;
                try {
                    keyValue = objectMapper.writeValueAsString(keyValueDetails);
                }
                catch(JsonProcessingException e)
                {
                    throw new ProjectCommonException(
                            ResponseCode.jsonDataFormatError.getErrorCode(),
                            ResponseCode.jsonDataFormatError.getErrorMessage(),
                            ResponseCode.CLIENT_ERROR.getResponseCode());
                }
                newRequest.put(JsonKey.VALUE,keyValue);
                newRequest.put(JsonKey.CREATED_ON, ProjectUtil.getFormattedDate());
                String tenantPreferenceDetailIds=ProjectUtil.getUniqueIdFromTimestamp(actorMessage.getEnv());
                newRequest.put(JsonKey.ID,ProjectUtil.getUniqueIdFromTimestamp(actorMessage.getEnv()));
                newRequest.put(JsonKey.ACTIVE,"Y");
                newRequest.put(JsonKey.CREATED_BY,request.get(JsonKey.CREATED_BY));
                TenantPreferenceDetails details=mapper.convertValue(newRequest,TenantPreferenceDetails.class);
                result = multiTenantDao.createTenantPreferenceData(details);

            }

        }
        return result;
    }

    // To update Tenant Info
    private void updateTenantInfo(Request actorMessage) {

        Map<String, Object> request = actorMessage.getRequest();
        List<String> errMsgs = new ArrayList<>();
        Response returnResponse = new Response();

        MultiTenant multiTenant = multiTenantDao.readTenantInfoById((String) request.get(JsonKey.ID));
        String orgId=multiTenant.getOrgId();

        Response response =
                cassandraOperation.getRecordById(orgDb.getKeySpace(), orgDb.getTableName(),orgId);
        List<Map<String, Object>> tenantInfoList =
                (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
        if(tenantInfoList.isEmpty())
        {
            errMsgs.add("No such Organisation exists in Cassandra Organisation Table");
            returnResponse.put(JsonKey.ORGANISATION_ID,orgId);
            returnResponse.put(JsonKey.ERROR_MSG,errMsgs);
        }
        else{
            Map<String,Object> tenantInfo = tenantInfoList.get(0);
            multiTenant = mapper.convertValue(tenantInfo, MultiTenant.class);
            tenantInfo = mapper.convertValue(multiTenant, Map.class);
            tenantInfo.put(JsonKey.ID,(String) request.get(JsonKey.ID));
            tenantInfo.put(JsonKey.ORG_ID,orgId);

            response = cassandraOperation.updateRecord(
                            tenantInfoDb.getKeySpace(), tenantInfoDb.getTableName(), tenantInfo);
            response.put(CaminoJsonKey.TENANT_INFO_ID,tenantInfo.get(JsonKey.ID));
        }

        sender().tell(returnResponse, self());
    }

    // To update Tenant Preference Details
    private void updateTenantPreferenceDetails(Request actorMessage) {

        Map<String, Object> request = actorMessage.getRequest();
        String requestedBy = (String) actorMessage.getContext().get(JsonKey.REQUESTED_BY);
        Response response;

        TenantPreferenceDetails tenantPreferenceDetails = multiTenantDao.readTenantPreferenceDetailById((String) request.get(JsonKey.ID));
        Map<String,Object> newRequest=mapper.convertValue(tenantPreferenceDetails,Map.class);

        if (request.containsKey(JsonKey.PAGE)
                && !StringUtils.isBlank((String) request.get(JsonKey.PAGE))) {
            newRequest.put(JsonKey.PAGE,(String) request.get(JsonKey.PAGE));
        }
        if (request.containsKey(JsonKey.KEY)
                && !StringUtils.isBlank((String) request.get(JsonKey.KEY))) {
            newRequest.put(JsonKey.KEY,(String) request.get(JsonKey.KEY));
        }
        if (request.containsKey(JsonKey.VALUE)
                && !StringUtils.isBlank((String) request.get(JsonKey.VALUE))) {
            newRequest.put(JsonKey.VALUE,(String) request.get(JsonKey.VALUE));
        }
        if (request.containsKey(JsonKey.ACTIVE)
                && !StringUtils.isBlank((String) request.get(JsonKey.ACTIVE))) {
            newRequest.put(JsonKey.ACTIVE,(String) request.get(JsonKey.ACTIVE));
        }

        newRequest.put(CaminoJsonKey.CHANGED_BY,requestedBy);
        newRequest.put(CaminoJsonKey.CHANGED_ON,ProjectUtil.getFormattedDate());

        response = cassandraOperation.updateRecord(
                tenantPreferenceDetailsDb.getKeySpace(), tenantPreferenceDetailsDb.getTableName(), newRequest);
        response.put(CaminoJsonKey.TENANT_PREFERENCE_DETAIL_ID,newRequest.get(JsonKey.ID));

        sender().tell(response, self());
    }

    // To get Tenant Info
    private void getTenantInfo(Request actorMessage) {


        Map<String, Object> request = actorMessage.getRequest();
        String homeUrl = (String) request.get(JsonKey.HOME_URL);
        Response response = multiTenantDao.readTenantInfoByHomeUrl(homeUrl);

        List<Map<String, Object>> tenantList =
                (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
        String newHomeUrl = null;

        // if Tenant Info is not found, change the url for rootOrg
        // example : RootOrg Url - www.sunbird.com
        // subOrg Url - www.sunbird.com/subOrg1
        // another subOrg url - www.sunbird.com/subOrg2
        // So remove the subOrg part from url and return the details of the rootOrg

        if (tenantList.isEmpty()) {
            newHomeUrl = homeUrl.substring(0, homeUrl.lastIndexOf('/'));
            response = multiTenantDao.readTenantInfoByHomeUrl(newHomeUrl);
        }
        tenantList =
                (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);

        if (tenantList.isEmpty()) {
            throw new ProjectCommonException(
                    ResponseCode.invalidHomeUrl.getErrorCode(),
                    ResponseCode.invalidHomeUrl.getErrorMessage(),
                    ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
        }


        Map<String,Object> multiTenant=tenantList.get(0);
        String orgId = (String) multiTenant.get(JsonKey.ORG_ID);

        Map<String,Object> tenantPreData=getTenantPreferenceDetailsByOrgId(orgId);

        response.put(CaminoJsonKey.TENANT_PREFERENCE_DETAILS,tenantPreData);
        sender().tell(response, self());
    }

    //To get Tenant Preference Details
    private Map<String,Object> getTenantPreferenceDetailsByOrgId(String orgId) {

        Map<String,Object> tenantPreData= new HashMap<>();
        ObjectMapper objectMapper=new ObjectMapper();
        try {
            Response tenantPreferenceList = multiTenantDao.readTenantPreferenceDetailsByOrgId(orgId);
            List<Map<String, Object>>  tenantPreferenceDetails=
                    (List<Map<String, Object>>) tenantPreferenceList.get(JsonKey.RESPONSE);
            Iterator<Map<String, Object>> iterator = tenantPreferenceDetails.iterator();
            while (iterator.hasNext()) {
                Map<String, Object> details = iterator.next();
                String page=(String)details.get(JsonKey.PAGE);
                String key=(String) details.get(JsonKey.KEY);
                String value=(String)details.get(JsonKey.VALUE);
                Map<String,Object> data=null;
                Map<String, Object> map1=null;
                if(tenantPreData.containsKey(page)) {
                    data=(Map<String ,Object>)tenantPreData.get(page);
                    map1 = objectMapper.readValue(value, Map.class);
                }
                else {
                    data=new HashMap<>();
                    map1 = objectMapper.readValue(value, Map.class);
                }
                map1.put(JsonKey.ID,(String)details.get(JsonKey.ID));
                map1.put(JsonKey.ACTIVE,(String)details.get(JsonKey.ACTIVE));
                map1.put(JsonKey.CREATED_BY,(String)details.get(JsonKey.CREATED_BY));
                map1.put(JsonKey.CREATED_ON,(String)details.get(JsonKey.CREATED_ON));
                map1.put(CaminoJsonKey.CHANGED_BY,(String)details.get(CaminoJsonKey.CHANGED_BY));
                map1.put(CaminoJsonKey.CHANGED_ON,(String)details.get(CaminoJsonKey.CHANGED_ON));
                data.put(key,map1);
                tenantPreData.put(page,data);
            }
        }
        catch (IOException e)
        {
            throw new ProjectCommonException(
                    ResponseCode.valueSyntaxError.getErrorCode(),
                    ResponseCode.valueSyntaxError.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }

        return tenantPreData;
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

        HashMap<String, Object> orgDetails=null;
        long count = (long) result.get(JsonKey.COUNT);
        if(count == 1){
            List<Object> content = (ArrayList<Object>) result.get(JsonKey.CONTENT);
            orgDetails = (HashMap<String, Object>) content.get(0);
            return orgDetails;
        }

        else if (count > 1) {
            List<Object> content = (ArrayList<Object>) result.get(JsonKey.CONTENT);
            Iterator iterator = content.iterator();
            while(iterator.hasNext()){
                orgDetails = (HashMap<String, Object>)iterator.next();
                boolean check=(boolean)orgDetails.get(JsonKey.IS_ROOT_ORG);
                if(check)
                    break;
            }
            return orgDetails;
        }
        else
        {
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
                multiTenantDao.readTenantInfoByHomeUrl(homeUrl);
        List<Map<String, Object>> tenantList =
                (List<Map<String, Object>>) tenantInfoResult.get(JsonKey.RESPONSE);
        if (!tenantList.isEmpty()) {
            throw new ProjectCommonException(
                    ResponseCode.homeUrlAlreadyExists.getErrorCode(),
                    ResponseCode.homeUrlAlreadyExists.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }
    }


    // To add new Tenant Preference Details to an existing Tenant
    public void addTenantPreferenceDetails(Request actorMessage) {

        Map<String, Object> request = actorMessage.getRequest();
        String requestedBy = (String) actorMessage.getContext().get(JsonKey.REQUESTED_BY);
        String homeUrl = (String) request.get(JsonKey.HOME_URL);

        Response tenantInfoResult =
                multiTenantDao.readTenantInfoByHomeUrl(homeUrl);
        List<Map<String, Object>> tenantList =
                (List<Map<String, Object>>) tenantInfoResult.get(JsonKey.RESPONSE);
        if (tenantList.isEmpty()) {
            throw new ProjectCommonException(
                    ResponseCode.invalidHomeUrl.getErrorCode(),
                    ResponseCode.invalidHomeUrl.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }
        Map<String,Object> multiTenant=tenantList.get(0);
        String orgId = (String) multiTenant.get(JsonKey.ORG_ID);
        request.put(JsonKey.ORG_ID,orgId);
        actorMessage.setRequest(request);
        Response response=createTenantPreferenceDetails(actorMessage);

        sender().tell(response, self());
    }


    }

//          ProjectLogger.log(
//                  "CourseBatchManagementActor:getEkStepContent: Not found course for ID = " + courseId,
//                  LoggerEnum.INFO.name());