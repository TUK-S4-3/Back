CREATE TABLE `job_stage_runs` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `job_id` BIGINT UNSIGNED NOT NULL,
  `stage` ENUM('KS_SFM', 'GS') NOT NULL,
  `container_id` VARCHAR(128) NULL,
  `status` ENUM('QUEUED', 'SUBMITTED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED') NOT NULL DEFAULT 'QUEUED',
  `config_key` VARCHAR(1024) NULL,
  `logs_prefix` VARCHAR(1024) NULL,
  `outputs_json` JSON NULL,
  `error_code` VARCHAR(50) NULL,
  `error_message` VARCHAR(255) NULL,
  `started_at` DATETIME(3) NULL,
  `ended_at` DATETIME(3) NULL,
  `created_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`id`),
  INDEX `idx_job_stage_runs_job_stage_created`(`job_id`, `stage`, `created_at`),
  INDEX `idx_job_stage_runs_status_created`(`status`, `created_at`),
  INDEX `idx_job_stage_runs_container_id`(`container_id`),
  CONSTRAINT `job_stage_runs_job_id_fkey`
    FOREIGN KEY (`job_id`) REFERENCES `jobs`(`id`)
    ON DELETE CASCADE ON UPDATE CASCADE
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
