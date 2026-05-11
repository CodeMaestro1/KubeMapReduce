-- migrations/0003_add_locality_config.sql
--
-- Adds support for data locality-aware scheduling.
-- stores the Kubernetes topology label key (e.g. topology.kubernetes.io/zone)
-- used to co-locate workers with MinIO pods.

BEGIN;

-- Add locality columns with defaults
ALTER TABLE SYSTEM_CONFIG
    ADD COLUMN IF NOT EXISTS locality_key TEXT NOT NULL DEFAULT 'topology.kubernetes.io/zone',
    ADD COLUMN IF NOT EXISTS locality_label_selector TEXT NOT NULL DEFAULT 'app.kubernetes.io/name=minio';

-- Update the seed row so existing clusters pick up the defaults
UPDATE SYSTEM_CONFIG SET 
    locality_key = 'topology.kubernetes.io/zone',
    locality_label_selector = 'app.kubernetes.io/name=minio'
WHERE config_id = 1;

COMMIT;
