-- Rooms carry an operator-set vanity name. It is nullable and deliberately not
-- unique: a room is fully usable unnamed, two rooms may share a name, and the id
-- remains the key everywhere. The hub owns the value and mirrors it to nodes.

ALTER TABLE conversations ADD COLUMN name TEXT;
