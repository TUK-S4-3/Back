ALTER TABLE `jobs`
  MODIFY COLUMN `status` ENUM(
    'QUEUED',
    'SUBMITTED',
    'RUNNING',
    'WAITING_GS',
    'SUCCEEDED',
    'FAILED',
    'CANCELLED'
  ) NOT NULL DEFAULT 'QUEUED';

ALTER TABLE `jobs`
  MODIFY COLUMN `stage` ENUM(
    'INSTANCE_CREATING',
    'IMAGESET_BUILDING',
    'SFM_FEATURE',
    'SFM_MATCH',
    'SFM_MAPPER',
    'SFM',
    'SFM_DONE',
    'UNDISTORT',
    'GS_TRAINING',
    'MESH_EXTRACTION',
    'FINALIZING',
    'UPLOADING',
    'DONE'
  ) NOT NULL DEFAULT 'INSTANCE_CREATING';

ALTER TABLE `jobs`
  ADD COLUMN `gs_batch_job_id` VARCHAR(128) NULL AFTER `batch_job_id`,
  ADD INDEX `idx_jobs_gs_batch_job_id`(`gs_batch_job_id`);
