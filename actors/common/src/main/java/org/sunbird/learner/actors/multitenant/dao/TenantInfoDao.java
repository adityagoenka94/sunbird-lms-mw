package org.sunbird.learner.actors.multitenant.dao;

import org.sunbird.common.models.response.Response;
import org.sunbird.models.multitenant.TenantPreference;

public interface TenantInfoDao {

    /**
     * Create tenant info for Camino Instance.
     *
     * @param multiTenant Multi Tenant information to be created
     * @return Response containing identifier of created Tenant info
     */

    Response createMultiTenantInfo(TenantPreference multiTenant);

    /**
     * Read Tenant Info for given identifier for Camino Instance.
     *
     * @param property name of the property on which you want to perform read operation
     * @param propertyValue value of the property on which you want to perform read operation
     * @return Tenant Info information
     */

    Response readMultiTenantInfoByProperty(String property,String propertyValue);

    /**
     * Read Tenant Info for given identifier for Camino Instance.
     *
     * @param id Tenant Info Id identifier
     * @return Tenant Info information
     */

    TenantPreference readMultiTenantInfoById(String id);

    /**
     * Update Tenant Info for given identifier for Camino Instance.
     *
     * @param multiTenant Multi Tenant information to be updated
     * @return Response containing identifier of updated Tenant info
     */

    Response updateMultiTenantInfo(TenantPreference multiTenant);

    /**
     * Delete Tenant Info for given identifier for Camino Instance.
     *
     * @param tenantId Multi Tenant id to be deleted
     * @return Response containing Success or Fail Report
     */

    Response deleteMultiTenantInfo(String tenantId);


}
