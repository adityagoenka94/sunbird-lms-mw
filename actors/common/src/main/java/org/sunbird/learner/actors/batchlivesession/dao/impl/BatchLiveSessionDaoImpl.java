package org.sunbird.learner.actors.batchlivesession.dao.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.CaminoJsonKey;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.batchlivesession.dao.BatchLiveSessionDao;
import org.sunbird.learner.util.CaminoUtil;
import org.sunbird.learner.util.Util;
import org.sunbird.models.course.batch.LiveSession;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BatchLiveSessionDaoImpl implements BatchLiveSessionDao {

    private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
    private Util.DbInfo courseBatchDb = CaminoUtil.dbInfoMap.get(JsonKey.COURSE_BATCH_DB);
    private Util.DbInfo batchLiveSessionDb = CaminoUtil.dbInfoMap.get(CaminoJsonKey.BATCH_LIVE_SESSION_DB);

    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public Response readCourseDetailsByBatchId(String batchId) {

        Map<String, Object> batch= new HashMap<>();

        batch.put(JsonKey.ID,batchId);
        Response courseBatchResult =
                cassandraOperation.getRecordsByProperties(
                        courseBatchDb.getKeySpace(), courseBatchDb.getTableName(), batch);

        return courseBatchResult;
    }

    @Override
    public Response createBatchLiveSession(LiveSession liveSession)
    {
        Map<String, Object> map = mapper.convertValue(liveSession, Map.class);
        return cassandraOperation.insertRecord(
                batchLiveSessionDb.getKeySpace(),batchLiveSessionDb.getTableName(),map);
    }

    @Override
    public Response readBatchLiveSessionsByProperty(String property, String propertyValue)
    {
        Map<String, Object> map = new HashMap<>();

        map.put(property,propertyValue);
        Response readBatchLiveSessionResult =
                cassandraOperation.getRecordsByProperties(
                        batchLiveSessionDb.getKeySpace(), batchLiveSessionDb.getTableName(), map);
        return readBatchLiveSessionResult;
    }

    @Override
    public Response readBatchLiveSessionsByLiveSessionId(String liveSessionId)
    {

        Response readBatchLiveSessionResult =
                cassandraOperation.getRecordById(
                        batchLiveSessionDb.getKeySpace(), batchLiveSessionDb.getTableName(), liveSessionId);
        return readBatchLiveSessionResult;
    }

    @Override
    public Response deleteBatchLiveSessionById(String id)
    {
        Response response =
                cassandraOperation.deleteRecord(
                        batchLiveSessionDb.getKeySpace(), batchLiveSessionDb.getTableName(),id);
        return response;
    }

    @Override
    public Response updateBatchLiveSessionDetais(LiveSession liveSession)
    {
        Map<String,Object> map = mapper.convertValue(liveSession,Map.class);
        Response updateBatchLiveSession =
                cassandraOperation.updateRecord(
                        batchLiveSessionDb.getKeySpace(), batchLiveSessionDb.getTableName(), map);
        return updateBatchLiveSession;
    }
}
