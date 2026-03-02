ALTER TABLE `scenes`
  ADD COLUMN `upload_id` CHAR(36) NULL;

ALTER TABLE `jobs`
  ADD COLUMN `upload_id` CHAR(36) NULL;
