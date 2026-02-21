-- CreateTable
CREATE TABLE `users` (
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `provider` ENUM('KAKAO', 'NAVER', 'GOOGLE') NOT NULL,
    `provider_id` VARCHAR(128) NOT NULL,
    `nickname` VARCHAR(50) NULL,
    `profile_image_url` VARCHAR(255) NULL,
    `status` ENUM('ACTIVE', 'DELETED') NOT NULL DEFAULT 'ACTIVE',
    `created_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `updated_at` DATETIME(3) NOT NULL,

    UNIQUE INDEX `uk_provider_provider_id`(`provider`, `provider_id`),
    UNIQUE INDEX `uk_nickname`(`nickname`),
    PRIMARY KEY (`id`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- CreateTable
CREATE TABLE `scenes` (
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `user_id` BIGINT UNSIGNED NOT NULL,
    `title` VARCHAR(100) NOT NULL DEFAULT 'Untitled Scene',
    `status` ENUM('DRAFT', 'UPLOADING', 'UPLOADED', 'PROCESSING', 'READY', 'FAILED', 'CANCELLED') NOT NULL DEFAULT 'DRAFT',
    `input_video_key` VARCHAR(1024) NULL,
    `sfm_result_key` VARCHAR(1024) NULL,
    `gaussian_splat_key` VARCHAR(1024) NULL,
    `mesh_key` VARCHAR(1024) NULL,
    `thumbnail_key` VARCHAR(1024) NULL,
    `created_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `updated_at` DATETIME(3) NOT NULL,
    `finished_at` DATETIME(3) NULL,

    INDEX `idx_scenes_user_created`(`user_id`, `created_at`),
    INDEX `idx_scenes_status`(`status`),
    PRIMARY KEY (`id`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- CreateTable
CREATE TABLE `jobs` (
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `scene_id` BIGINT UNSIGNED NOT NULL,
    `batch_job_id` VARCHAR(128) NULL,
    `status` ENUM('QUEUED', 'SUBMITTED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED') NOT NULL DEFAULT 'QUEUED',
    `stage` ENUM('INSTANCE_CREATING', 'IMAGESET_BUILDING', 'SFM', 'GS_TRAINING', 'MESH_EXTRACTION', 'FINALIZING') NOT NULL DEFAULT 'INSTANCE_CREATING',
    `progress_percent` TINYINT UNSIGNED NOT NULL DEFAULT 0,
    `cancel_requested` BOOLEAN NOT NULL DEFAULT false,
    `attempt` INTEGER UNSIGNED NOT NULL DEFAULT 0,
    `error_code` VARCHAR(50) NULL,
    `error_message` VARCHAR(255) NULL,
    `created_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `updated_at` DATETIME(3) NOT NULL,
    `started_at` DATETIME(3) NULL,
    `ended_at` DATETIME(3) NULL,

    INDEX `idx_jobs_scene_created`(`scene_id`, `created_at`),
    INDEX `idx_jobs_status_created`(`status`, `created_at`),
    INDEX `idx_jobs_stage`(`stage`),
    UNIQUE INDEX `uk_batch_job_id`(`batch_job_id`),
    PRIMARY KEY (`id`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- CreateTable
CREATE TABLE `posts` (
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `user_id` BIGINT UNSIGNED NOT NULL,
    `scene_id` BIGINT UNSIGNED NOT NULL,
    `title` VARCHAR(100) NOT NULL,
    `status` ENUM('PUBLISHED', 'DELETED') NOT NULL DEFAULT 'PUBLISHED',
    `like_count` INTEGER UNSIGNED NOT NULL DEFAULT 0,
    `download_count` INTEGER UNSIGNED NOT NULL DEFAULT 0,
    `share_uuid` CHAR(36) NOT NULL,
    `created_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `updated_at` DATETIME(3) NOT NULL,

    INDEX `idx_posts_user_created`(`user_id`, `created_at`),
    INDEX `idx_posts_scene`(`scene_id`),
    INDEX `idx_posts_created`(`created_at`),
    INDEX `idx_posts_like`(`like_count`, `created_at`),
    INDEX `idx_posts_download`(`download_count`, `created_at`),
    UNIQUE INDEX `uk_posts_share_uuid`(`share_uuid`),
    PRIMARY KEY (`id`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- CreateTable
CREATE TABLE `likes` (
    `user_id` BIGINT UNSIGNED NOT NULL,
    `post_id` BIGINT UNSIGNED NOT NULL,
    `created_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),

    INDEX `idx_likes_post_created`(`post_id`, `created_at`),
    INDEX `idx_likes_user_created`(`user_id`, `created_at`),
    PRIMARY KEY (`user_id`, `post_id`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- AddForeignKey
ALTER TABLE `scenes` ADD CONSTRAINT `scenes_user_id_fkey` FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE `jobs` ADD CONSTRAINT `jobs_scene_id_fkey` FOREIGN KEY (`scene_id`) REFERENCES `scenes`(`id`) ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE `posts` ADD CONSTRAINT `posts_user_id_fkey` FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE `posts` ADD CONSTRAINT `posts_scene_id_fkey` FOREIGN KEY (`scene_id`) REFERENCES `scenes`(`id`) ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE `likes` ADD CONSTRAINT `likes_user_id_fkey` FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE `likes` ADD CONSTRAINT `likes_post_id_fkey` FOREIGN KEY (`post_id`) REFERENCES `posts`(`id`) ON DELETE CASCADE ON UPDATE CASCADE;
