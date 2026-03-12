-- Community posting refactor
-- NOTE:
-- Scene result columns are intentionally kept in this step for read fallback.
-- If existing posts rows are present, `posts.job_id` backfill requires
-- manual mapping because one scene can own multiple jobs.

ALTER TABLE `jobs`
  ADD COLUMN `pipeline` VARCHAR(50) NOT NULL DEFAULT '3dgs' AFTER `upload_id`,
  ADD COLUMN `image_count` INTEGER UNSIGNED NOT NULL DEFAULT 0 AFTER `pipeline`,
  ADD COLUMN `overlap` INTEGER UNSIGNED NOT NULL DEFAULT 0 AFTER `image_count`,
  ADD COLUMN `iteration` INTEGER UNSIGNED NOT NULL DEFAULT 0 AFTER `overlap`,
  ADD COLUMN `sfm_result_key` VARCHAR(1024) NULL AFTER `iteration`,
  ADD COLUMN `gaussian_splat_key` VARCHAR(1024) NULL AFTER `sfm_result_key`,
  ADD COLUMN `mesh_key` VARCHAR(1024) NULL AFTER `gaussian_splat_key`,
  ADD COLUMN `thumbnail_key` VARCHAR(1024) NULL AFTER `mesh_key`,
  ADD INDEX `idx_jobs_pipeline`(`pipeline`);

ALTER TABLE `posts`
  DROP FOREIGN KEY `posts_scene_id_fkey`;

ALTER TABLE `posts`
  ADD COLUMN `job_id` BIGINT UNSIGNED NOT NULL AFTER `user_id`,
  ADD UNIQUE INDEX `uk_posts_job_id`(`job_id`),
  ADD CONSTRAINT `posts_job_id_fkey`
    FOREIGN KEY (`job_id`) REFERENCES `jobs`(`id`)
    ON DELETE CASCADE ON UPDATE CASCADE,
  DROP INDEX `idx_posts_scene`,
  DROP COLUMN `scene_id`;
