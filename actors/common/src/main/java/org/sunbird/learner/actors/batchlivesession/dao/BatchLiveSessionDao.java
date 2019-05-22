package org.sunbird.learner.actors.batchlivesession.dao;

import org.sunbird.common.models.response.Response;
import org.sunbird.models.course.batch.LiveSession;

import java.util.List;

public interface BatchLiveSessionDao {

    /**
     * Read Tenant Info for given identifier for Camino Instance.
     *
     * @param homeUrl Home Url identifier
     * @return Tenant Info information
     */

    Response readCourseDetailsByBatchId(String batchId);


    Response createBatchLiveSession(LiveSession liveSession);

    Response readBatchLiveSessionsByProperty(String property,String propertyValue);

    Response readBatchLiveSessionsByLiveSessionId(String liveSessionId);

    Response deleteBatchLiveSessionById(String id);

    Response updateBatchLiveSessionDetais(LiveSession liveSession);

}
