-- migrations/0003_add_locality_config.sql
--
-- Adds support for data locality-aware scheduling.
-- stores the Kubernetes topology label key (e.g. topology.kubernetes.io/zone)
-- used to co-locate workers with MinIO pods.

BEGIN;

-- Add locality_key column with a default of zone-level affinity
ALTER TABLE SYSTEM_CONFIG
    ADD COLUMN IF NOT EXISTS locality_key TEXT NOT NULL DEFAULT 'topology.kubernetes.io/zone';

-- Update the seed row so existing clusters pick up the default
UPDATE SYSTEM_CONFIG SET locality_key = 'topology.kubernetes.io/zone' WHERE config_id = 1 AND (locality_key IS NULL OR locality_key = '');

COMMIT;
