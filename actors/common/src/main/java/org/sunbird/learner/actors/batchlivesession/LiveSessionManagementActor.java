package org.sunbird.learner.actors.batchlivesession;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.*;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.actors.batchlivesession.dao.BatchLiveSessionDao;
import org.sunbird.learner.actors.batchlivesession.dao.impl.BatchLiveSessionDaoImpl;
import org.sunbird.learner.util.Util;
import org.sunbird.models.course.batch.LiveSession;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@ActorConfig(
        tasks = {
                "createBatchLiveSessions",
                "updateBatchLiveSession",
                "readBatchLiveSessions",
                "deleteBatchLiveSession"
        },
        asyncTasks = {}
)
public class LiveSessionManagementActor extends BaseActor {

    ObjectMapper mapper = new ObjectMapper();
    private BatchLiveSessionDao batchLiveSessionDao = new BatchLiveSessionDaoImpl();
    private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private SimpleDateFormat simpleDateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSSZ");

    @Override
    public void onReceive(Request request) throws Throwable {

        Util.initializeContext(request, CaminoTelemetryEnvKey.BATCH_LIVE_SESSION);
        ExecutionContext.setRequestId(request.getRequestId());

        String requestedOperation = request.getOperation();
        switch (requestedOperation) {
            case "createBatchLiveSessions":
                createBatchLiveSessions(request);
                break;
            case "updateBatchLiveSession":
                updateBatchLiveSession(request);
                break;
            case "readBatchLiveSessions":
                readBatchLiveSessions(request);
                break;
            case "deleteBatchLiveSession":
                deleteBatchLiveSession(request);
                break;
            default:
                onReceiveUnsupportedOperation(request.getOperation());
                break;
        }
    }

    // To create Batch Live Sessions
    private void createBatchLiveSessions(Request actorMessage) {

        ProjectLogger.log("Create Batch Live Session Api Called");
        Map<String, Object> request = actorMessage.getRequest();
        String requestedBy = (String) actorMessage.getContext().get(JsonKey.REQUESTED_BY);

        LiveSession liveSession = null;
        Response result=null;

        validateContentId((String)request.get(JsonKey.CONTENT_ID));

        String liveSessionId = ProjectUtil.getUniqueIdFromTimestamp(actorMessage.getEnv());
        String batchId = (String) request.get(JsonKey.BATCH_ID);


        Map<String,Object> courseBatchdetail = validateBatchId(batchId);
        String courseId = (String) courseBatchdetail.get(JsonKey.COURSE_ID);
        request.put(JsonKey.COURSE_ID,courseId);

        validateSessionDatesWithBatchDates(courseBatchdetail,request);

                request.put(JsonKey.ID,liveSessionId);

                liveSession = mapper.convertValue(request, LiveSession.class);
                liveSession.setCreatedBy(requestedBy);
                liveSession.setCreatedDate(ProjectUtil.getFormattedDate());
                liveSession.setStatus((int) courseBatchdetail.get(JsonKey.STATUS));
                liveSession.setLiveSessionId(liveSessionId);

                result = batchLiveSessionDao.createBatchLiveSession(liveSession);
                result.put(JsonKey.ID,liveSessionId);

        ProjectLogger.log(
                "LiveSessionManagementActor:createBatchLiveSessions():  Batch Live Session created successfully. " + result,
                LoggerEnum.INFO.name());
        sender().tell(result, self());
    }


    // To update Batch Live Session
    private void updateBatchLiveSession(Request actorMessage) {

        ProjectLogger.log("Update Batch Live Session Api Called");
        Map<String, Object> request = actorMessage.getRequest();
        Response liveSessionDetails = null;


        if(!StringUtils.isBlank((String)request.get(CaminoJsonKey.LIVE_SESSION_ID))) {
            liveSessionDetails=batchLiveSessionDao.readBatchLiveSessionsByLiveSessionId((String)request.get(CaminoJsonKey.LIVE_SESSION_ID));
        }
        else if(!StringUtils.isBlank((String)request.get(JsonKey.CONTENT_ID))) {
            liveSessionDetails=batchLiveSessionDao.readBatchLiveSessionsByProperty(JsonKey.CONTENT_ID,(String)request.get(JsonKey.CONTENT_ID));
        }

        List<Object> liveSessions =
                (List<Object>) liveSessionDetails.get(JsonKey.RESPONSE);
        if (liveSessions.isEmpty()) {
            ProjectLogger.log(
                    "LiveSessionManagementActor:updateBatchLiveSession(): No LiveSession found with specified ID in batch_live_session table",
                    LoggerEnum.ERROR.name());
            throw new ProjectCommonException(
                    ResponseCode.resourceNotFound.getErrorCode(),
                    ResponseCode.resourceNotFound.getErrorMessage(),
                    ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
        }

        Map<String,Object> liveSessionDetail = (Map<String, Object>) liveSessions.get(0);
        LiveSession liveSession = mapper.convertValue(liveSessionDetail,LiveSession.class);

        if(!StringUtils.isBlank((String)request.get(CaminoJsonKey.LIVE_SESSION_URL))) {
            liveSession.setLiveSessionUrl((String)request.get(CaminoJsonKey.LIVE_SESSION_URL));
        }
        if(!StringUtils.isBlank((String)request.get(JsonKey.START_TIME))) {
            liveSession.setLiveSessionUrl((String)request.get(JsonKey.START_TIME));
        }
        if(!StringUtils.isBlank((String)request.get(JsonKey.END_TIME))) {
            liveSession.setLiveSessionUrl((String)request.get(JsonKey.END_TIME));
        }

        Map<String,Object> courseBatchdetail = validateBatchId(liveSession.getBatchId());
        validateSessionDatesWithBatchDates(courseBatchdetail,liveSessionDetail);
        liveSession.setStatus((int) courseBatchdetail.get(JsonKey.STATUS));

        liveSession.setUpdatedDate(ProjectUtil.getFormattedDate());

        Response result = batchLiveSessionDao.updateBatchLiveSessionDetais(liveSession);

        ProjectLogger.log(
                "LiveSessionManagementActor:updateBatchLiveSession():  Batch Live Session updated successfully. " + result,
                LoggerEnum.INFO.name());
        sender().tell(result, self());
    }


    // To get Batch Live Session
    private void readBatchLiveSessions(Request actorMessage) {

        ProjectLogger.log("Read Batch Live Session Api Called");
        Map<String, Object> request = actorMessage.getRequest();
        String batchId = (String) request.get(JsonKey.BATCH_ID);

        Response batchLiveSessionRead = batchLiveSessionDao.readBatchLiveSessionsByProperty(JsonKey.BATCH_ID, batchId);
        List<Map<String, Object>> batchLiveSessionList =
                (List<Map<String, Object>>) batchLiveSessionRead.get(JsonKey.RESPONSE);
        if ((batchLiveSessionList.isEmpty())) {
            ProjectLogger.log(
                    "LiveSessionManagementActor:readBatchLiveSessions(): No LiveSession found with specified ID in batch_live_session table",
                    LoggerEnum.ERROR.name());
            throw new ProjectCommonException(
                    ResponseCode.invalidCourseBatchId.getErrorCode(),
                    ResponseCode.invalidCourseBatchId.getErrorMessage(),
                    ResponseCode.invalidCourseBatchId.getResponseCode());
        }
        List<Object> sessionDetails =new ArrayList<>();

        Iterator<Map<String, Object>> iterator = batchLiveSessionList.iterator();
        while (iterator.hasNext()) {
            Map<String, Object> batchLiveSession = iterator.next();

            String unitId = (String) batchLiveSession.get(CaminoJsonKey.UNIT_ID);

            List<Object> contentDetails=new ArrayList<>();
            Map<String, Object> map1=new HashMap<>();

            for(Object sessionUnits : sessionDetails) {
                Map<String, Object> units = (Map<String, Object>) sessionUnits;
                if (units.containsValue(unitId)) {
                    contentDetails = (List<Object>) units.get(CaminoJsonKey.CONTENT_DETAILS);
                    sessionDetails.remove(sessionUnits);
                    break;
                }
            }
                contentDetails.add(batchLiveSession);
                map1.put(CaminoJsonKey.UNIT_ID,unitId);
                map1.put(CaminoJsonKey.CONTENT_DETAILS,contentDetails);
                sessionDetails.add(map1);
        }

        Map <String, Object> result=new HashMap<>();
        Map<String, Object> data2 = batchLiveSessionList.get(0);
        result.put(JsonKey.COURSE_ID, data2.get(JsonKey.COURSE_ID));
        result.put(JsonKey.BATCH_ID, data2.get(JsonKey.BATCH_ID));
        result.put(CaminoJsonKey.SESSION_DETAILS,sessionDetails);

        Response response = new Response();
        response.put(JsonKey.RESPONSE, result);

        ProjectLogger.log(
                "LiveSessionManagementActor:readBatchLiveSessions():  Batch Live Session read successfully. " + response,
                LoggerEnum.INFO.name());
        sender().tell(response,self());
    }

    // To delete Batch Live Session
    private void deleteBatchLiveSession(Request actorMessage) {

        ProjectLogger.log("Delete Batch Live Session Api Called");
        Map<String, Object> request = actorMessage.getRequest();
        String liveSessionId = validateSessionByContentId((String)request.get(JsonKey.CONTENT_ID));

        Response response = batchLiveSessionDao.deleteBatchLiveSessionById(liveSessionId);
        ProjectLogger.log(
                "LiveSessionManagementActor:deleteBatchLiveSession():  Batch Live Session deleted successfully. " + response,
                LoggerEnum.INFO.name());
        sender().tell(response, self());
    }

    private Map<String,Object> validateBatchId(String batchId) {

        Response courseBatchDetails = batchLiveSessionDao.readCourseDetailsByBatchId(batchId);
        List<Map<String, Object>> courseBatchList =
                (List<Map<String, Object>>) courseBatchDetails.get(JsonKey.RESPONSE);
        if (courseBatchList.isEmpty()) {
            ProjectLogger.log(
                    "LiveSessionManagementActor:validateBatchId():  Course with Batch Id = " + batchId + " does not exist in course_batch table",
                    LoggerEnum.ERROR.name());
            throw new ProjectCommonException(
                    ResponseCode.invalidCourseBatchId.getErrorCode(),
                    ResponseCode.invalidCourseBatchId.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }
        Map<String,Object> courseBatch = courseBatchList.get(0);

        if (ProjectUtil.ProgressStatus.COMPLETED.getValue() == (int)courseBatch.get(JsonKey.STATUS)) {
            ProjectLogger.log(
                    "LiveSessionManagementActor:validateBatchId(): The Batch has already completed.",
                    LoggerEnum.ERROR.name());
            throw new ProjectCommonException(
                    ResponseCode.courseBatchEndDateError.getErrorCode(),
                    ResponseCode.courseBatchEndDateError.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }

        return courseBatch;
    }


    private void validateContentId(String contentId) {

        Response liveSessionDetails = batchLiveSessionDao.readBatchLiveSessionsByProperty(JsonKey.CONTENT_ID,contentId);
        List<Object> liveSessionList =
                (List<Object>) liveSessionDetails.get(JsonKey.RESPONSE);
        if (!liveSessionList.isEmpty()) {
            ProjectLogger.log(
                    "LiveSessionManagementActor:validateContentId():  LiveSession with Content Id = " + contentId + " already exists in batch_live_session table",
                    LoggerEnum.ERROR.name());
            throw new ProjectCommonException(
                    ResponseCode.liveSessionAlreadyExists.getErrorCode(),
                    ResponseCode.liveSessionAlreadyExists.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }
    }


    private String validateSessionByContentId(String contentId) {

        Response liveSessionDetails = batchLiveSessionDao.readBatchLiveSessionsByProperty(JsonKey.CONTENT_ID,contentId);
        List<Map<String,Object>> liveSessionList =
                (List<Map<String,Object>>) liveSessionDetails.get(JsonKey.RESPONSE);
        if (liveSessionList.isEmpty()) {
            ProjectLogger.log(
                    "LiveSessionManagementActor:validateSessionByContentId():  LiveSession with Content Id = " + contentId + " does not exists in batch_live_session table",
                    LoggerEnum.ERROR.name());
            throw new ProjectCommonException(
                    ResponseCode.resourceNotFound.getErrorCode(),
                    ResponseCode.resourceNotFound.getErrorMessage(),
                    ResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
        }

        String liveSessionId = (String)((liveSessionList.get(0)).get(JsonKey.ID));
        return  liveSessionId;
    }

    private void validateSessionDatesWithBatchDates(Map<String,Object> courseBatchdetail, Map<String, Object> request) {

        String batchStartDate = (String) courseBatchdetail.get(JsonKey.START_DATE);
        String batchEndDate = (String) courseBatchdetail.get(JsonKey.END_DATE);

        String startTime = (String) request.get(JsonKey.START_TIME);
        String endTime = (String) request.get(JsonKey.END_TIME);


        String newStartTime =null;
        String newEndTime =null;

        try {
            newStartTime = startTime.substring(0, 10);
            newEndTime = endTime.substring(0, 10);
        }
        catch (IndexOutOfBoundsException e) {
            ProjectLogger.log(
                    "LiveSessionManagementActor:createBatchLiveSessions(): The format of the start date or end date is not correct. Correct format (yyyy-MM-dd HH:mm:ss:SSSZ)",
                    LoggerEnum.ERROR.name());
            throw new ProjectCommonException(
                    ResponseCode.datePatternError.getErrorCode(),
                    ResponseCode.datePatternError.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }

        Date checkStartDate=null;
        Date checkEndDate=null;
        Date checkStartDateTime=null;
        Date checkEndDateTime=null;
        Date batchStartDate1=null;
        Date batchEndDate1=null;

        try {
            // liveSession date
            checkStartDate = simpleDateFormat.parse(newStartTime);
            checkEndDate = simpleDateFormat.parse(newEndTime);
            checkStartDateTime = simpleDateTimeFormat.parse(startTime);
            checkEndDateTime = simpleDateTimeFormat.parse(endTime);
            //batch date
            batchStartDate1 = simpleDateFormat.parse(batchStartDate);
            batchEndDate1 = simpleDateFormat.parse(batchEndDate);
        }
        catch(ParseException e) {
            ProjectLogger.log(
                    "LiveSessionManagementActor:createBatchLiveSessions(): The format of the start date or end date is not correct. Correct format (yyyy-MM-dd HH:mm:ss:SSSZ)",
                    LoggerEnum.ERROR.name());
            throw new ProjectCommonException(
                    ResponseCode.datePatternError.getErrorCode(),
                    ResponseCode.datePatternError.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }

        if(checkStartDateTime.after(checkEndDateTime) || checkStartDate.before(batchStartDate1) || checkEndDate.after(batchEndDate1))
        {
            ProjectLogger.log(
                    "LiveSessionManagementActor:createBatchLiveSessions(): The start date or end date is not within the course batch period.",
                    LoggerEnum.ERROR.name());
            throw new ProjectCommonException(
                    ResponseCode.invalidDateRange.getErrorCode(),
                    ResponseCode.invalidDateRange.getErrorMessage(),
                    ResponseCode.CLIENT_ERROR.getResponseCode());
        }

    }


}
