ALTER TABLE `users`
  ADD COLUMN `profile_image_key` VARCHAR(1024) NULL AFTER `profile_image_url`,
  ADD COLUMN `profile_image_updated_at` DATETIME(3) NULL AFTER `profile_image_key`;
