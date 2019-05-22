package org.sunbird.learner.actors.multitenant.dao;

import org.sunbird.common.models.response.Response;

import org.sunbird.models.multitenant.MultiTenant;
import org.sunbird.models.multitenant.TenantPreferenceDetails;

public interface MultiTenantDao {

    /**
     * Create tenant info for Camino Instance.
     *
     * @param multiTenant Multi Tenant information to be created
     * @return Response containing identifier of created Tenant info
     */

    Response createTenantInfo(MultiTenant multiTenant);

    /**
     * Create tenant preference details for Camino Instance.
     *
     * @param tenantPreferenceData Multi Tenant Preference details information as map to be created
     * @return Response containing identifier of created Tenant Preference Details
     */

    Response createTenantPreferenceData(TenantPreferenceDetails tenantPreferenceData);

    /**
     * Read Tenant Info for given identifier for Camino Instance.
     *
     * @param homeUrl Home Url identifier
     * @return Tenant Info information
     */

    Response readTenantInfoByHomeUrl(String homeUrl);

    /**
     * Read Tenant Info for given identifier for Camino Instance.
     *
     * @param orgId Organisation Id identifier
     * @return Tenant Info information
     */

    Response readTenantInfoByOrgId(String orgId);
    /**
     * Read Tenant Preference Details for given identifier for Camino Instance.
     *
     * @param orgId Org Id identifier
     * @return Tenant Preference Details information
     */

    Response readTenantPreferenceDetailsByOrgId(String orgId);

    /**
     * Read Tenant Info for given identifier for Camino Instance.
     *
     * @param id Tenant Info Id identifier
     * @return Tenant Info information
     */

    MultiTenant readTenantInfoById(String id);

    /**
     * Read Tenant Preference Details for given identifier for Camino Instance.
     *
     * @param id Tenant Preference Details Id identifier
     * @return Tenant Preference Details information
     */

    TenantPreferenceDetails readTenantPreferenceDetailById(String id);

}
