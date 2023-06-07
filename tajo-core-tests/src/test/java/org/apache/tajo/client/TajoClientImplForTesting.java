package org.apache.tajo.client;

import org.apache.tajo.service.ServiceTracker;


import java.sql.SQLException;

public class TajoClientImplForTesting extends TajoClientImpl{
    public TajoClientImplForTesting(ServiceTracker tracker, String baseDatabase, QueryClientImpl queryClient, CatalogAdminClientImpl catalogAdminClient) throws SQLException {
        super(tracker, baseDatabase);
        this.queryClient = queryClient;
        this.catalogClient = catalogAdminClient;
    }
}
