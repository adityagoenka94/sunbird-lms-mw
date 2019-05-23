package org.sunbird.learner.actors.batchlivesession.dao;

import org.sunbird.common.models.response.Response;
import org.sunbird.models.course.batch.LiveSession;

import java.util.List;
import java.util.Map;

public interface BatchLiveSessionDao {

    /**
     * Create Batch Live Session for Camino Instance.
     *
     * @param liveSession Batch Live Session information to be created
     * @return Response containing identifier of created Tenant info
     */

    Response createBatchLiveSession(LiveSession liveSession);

    /**
     * Read Course Details for Camino Instance.
     *
     * @param batchId Batch Id identifier
     * @return Response containing details of the Batch's Course
     */

    Response readCourseDetailsByBatchId(String batchId);

    /**
     * Read Batch Live Session for Camino Instance.
     *
     * @param property name of the property on which you want to perform read operation
     * @param propertyValue value of the property on which you want to perform read operation
     * @return Batch Live Sessions information
     */

    Response readBatchLiveSessionsByProperty(String property,String propertyValue);

    /**
     * Read Batch Live Session for Camino Instance.
     *
     * @param liveSessionId Live Session Id identifier
     * @return Response containing details of the Batch Live Session
     */

    Response readBatchLiveSessionsByLiveSessionId(String liveSessionId);

    /**
     * delete Batch Live Session for Camino Instance.
     *
     * @param liveSessionId Live Session Id identifier
     * @return Response containing Success or Fail Report
     */

    Response deleteBatchLiveSessionById(String liveSessionId);

    /**
     * Update Batch Live Session for Camino Instance.
     *
     * @param liveSession Batch Live Session information to be updated
     * @return Response containing identifier of updated Tenant info
     */

    Response updateBatchLiveSessionDetais(LiveSession liveSession);

}
