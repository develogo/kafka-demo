-- One role per service, each owning exactly one schema.
--
-- This is the ownership boundary made enforceable rather than merely
-- documented: payment-service connects as payments_service, which has no
-- rights on orders.orders at all. A service that wants to know something about
-- another aggregate has to learn it from an event.
--
-- Every role shares one password because this is a demo on a laptop.

CREATE ROLE orders_service        LOGIN PASSWORD 'demo';
CREATE ROLE payments_service      LOGIN PASSWORD 'demo';
CREATE ROLE inventory_service     LOGIN PASSWORD 'demo';
CREATE ROLE shipments_service     LOGIN PASSWORD 'demo';
CREATE ROLE notifications_service LOGIN PASSWORD 'demo';
CREATE ROLE projection_service    LOGIN PASSWORD 'demo';

CREATE SCHEMA orders        AUTHORIZATION orders_service;
CREATE SCHEMA payments      AUTHORIZATION payments_service;
CREATE SCHEMA inventory     AUTHORIZATION inventory_service;
CREATE SCHEMA shipments     AUTHORIZATION shipments_service;
CREATE SCHEMA notifications AUTHORIZATION notifications_service;
CREATE SCHEMA projection    AUTHORIZATION projection_service;

-- The tables are created by the service that owns the schema, on start-up.
-- The one cross-schema grant lives with the projection that gives it away:
-- see projectionDDL in internal/services/projection.go.
