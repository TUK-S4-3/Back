ALTER TABLE `posts`
  ADD COLUMN `thumbnail_key` VARCHAR(1024) NULL AFTER `share_uuid`,
  ADD COLUMN `thumbnail_updated_at` DATETIME(3) NULL AFTER `thumbnail_key`;
