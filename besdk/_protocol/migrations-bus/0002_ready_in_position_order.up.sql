-- A subscription takes its due deliveries in position order (P6.23, P6.28). Ordered by not_before, the index let the
-- planner fetch and sort every waiting row when the statistics lagged behind a sudden backlog (about 10 ms a claim
-- with 20 000 waiting, 0.1 ms once analysed); ordered by seq, the first due delivery is near the start of the index
-- whatever the statistics say.
DROP INDEX be_bus_delivery_due;
CREATE INDEX be_bus_delivery_ready ON be_bus_delivery (durable, seq) WHERE NOT in_flight;
