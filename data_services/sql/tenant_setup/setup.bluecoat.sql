/*
The following indexes are created because they are needed for the Bluecoat
automated bookings reconciliation process.
*/
ALTER TABLE bluecoat.APP_OPPORTUNITIES ADD INDEX idx_AppOpportunities_isSubordinate (ISSUBORDINATE);
ALTER TABLE bluecoat.APP_OPPORTUNITIES ADD INDEX idx_AppOpportunities_salesStage (FLOWS_SALESSTAGES_STATE_NAME);
ALTER TABLE bluecoat.APP_OPPORTUNITIES ADD INDEX idx_AppOpportunities_displayName (DISPLAYNAME);
