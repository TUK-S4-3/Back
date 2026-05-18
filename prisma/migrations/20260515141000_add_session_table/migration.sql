CREATE TABLE `session` (
  `id` VARCHAR(191) NOT NULL,
  `sid` VARCHAR(191) NOT NULL,
  `data` TEXT NOT NULL,
  `expires_at` DATETIME(3) NOT NULL,

  UNIQUE INDEX `Session_sid_key`(`sid`),
  PRIMARY KEY (`id`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
