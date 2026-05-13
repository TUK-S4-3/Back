ALTER TABLE `jobs`
  MODIFY COLUMN `stage` ENUM(
    'INSTANCE_CREATING',
    'IMAGESET_BUILDING',
    'SFM_FEATURE',
    'SFM_MATCH',
    'SFM_MAPPER',
    'SFM',
    'UNDISTORT',
    'GS_TRAINING',
    'MESH_EXTRACTION',
    'FINALIZING',
    'UPLOADING',
    'DONE'
  ) NOT NULL DEFAULT 'INSTANCE_CREATING';

CREATE TABLE `keyframe_sets` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `scene_id` BIGINT UNSIGNED NOT NULL,
  `version` INTEGER UNSIGNED NOT NULL,
  `status` ENUM('PENDING', 'RUNNING', 'READY', 'FAILED', 'ARCHIVED') NOT NULL DEFAULT 'PENDING',
  `storage_prefix` VARCHAR(1024) NOT NULL,
  `selected_frames_prefix` VARCHAR(1024) NULL,
  `selected_frames_csv_key` VARCHAR(1024) NULL,
  `metrics_key` VARCHAR(1024) NULL,
  `config_key` VARCHAR(1024) NULL,
  `frame_index_plot_key` VARCHAR(1024) NULL,
  `timeline_comparison_key` VARCHAR(1024) NULL,
  `selected_frame_count` INTEGER UNSIGNED NOT NULL DEFAULT 0,
  `config_hash` VARCHAR(128) NULL,
  `config_json` JSON NULL,
  `error_message` VARCHAR(255) NULL,
  `created_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` DATETIME(3) NOT NULL,

  UNIQUE INDEX `uk_keyframe_sets_scene_version`(`scene_id`, `version`),
  INDEX `idx_keyframe_sets_scene_status_created`(`scene_id`, `status`, `created_at`),
  INDEX `idx_keyframe_sets_status`(`status`),
  PRIMARY KEY (`id`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

ALTER TABLE `scenes`
  ADD COLUMN `active_keyframe_set_id` BIGINT UNSIGNED NULL,
  ADD INDEX `idx_scenes_active_keyframe_set_id`(`active_keyframe_set_id`);

ALTER TABLE `jobs`
  ADD COLUMN `keyframe_set_id` BIGINT UNSIGNED NULL,
  ADD COLUMN `source_job_id` BIGINT UNSIGNED NULL,
  ADD INDEX `idx_jobs_keyframe_set_id`(`keyframe_set_id`);

ALTER TABLE `jobs`
  ADD INDEX `idx_jobs_source_job_id`(`source_job_id`);

ALTER TABLE `keyframe_sets`
  ADD CONSTRAINT `keyframe_sets_scene_id_fkey`
  FOREIGN KEY (`scene_id`) REFERENCES `scenes`(`id`) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE `scenes`
  ADD CONSTRAINT `scenes_active_keyframe_set_id_fkey`
  FOREIGN KEY (`active_keyframe_set_id`) REFERENCES `keyframe_sets`(`id`) ON DELETE SET NULL ON UPDATE CASCADE;

ALTER TABLE `jobs`
  ADD CONSTRAINT `jobs_keyframe_set_id_fkey`
  FOREIGN KEY (`keyframe_set_id`) REFERENCES `keyframe_sets`(`id`) ON DELETE SET NULL ON UPDATE CASCADE;
