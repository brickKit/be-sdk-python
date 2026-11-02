-- A claim takes the first expired lease in position order (P6.28), along the in-flight rows only. Ordered by
-- lease_until, the index let the planner walk the subscription's whole backlog through the primary key and filter
-- it when the table had never been analysed (in_flight then estimated as true for half the rows): 1.7 ms a claim with
-- 20 000 waiting against 0.16 ms with 20, planned for its own parameters. Ordered by seq it gives the position order
-- the claim needs, and the planner takes it whatever the statistics say. The give-up sweep looks at the same
-- in-flight rows.
DROP INDEX be_bus_delivery_lease;
CREATE INDEX be_bus_delivery_in_flight ON be_bus_delivery (durable, seq) WHERE in_flight;
