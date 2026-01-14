-- update city counts
UPDATE count_info, 
    (
        SELECT server_id, city_id, COUNT(*) AS city_counts FROM chain_info WHERE valid = 1 AND user_id IS NOT NULL GROUP BY server_id, city_id
    ) x
SET count_info.count = x.city_counts
WHERE count_info.server_id = x.server_id AND count_info.city_id = x.city_id;

-- update server user info
UPDATE server_user_info,
    (
        SELECT server_id, user_id, SUM(CASE WHEN valid = 1 THEN 1 ELSE 0 END) AS correct, SUM(CASE WHEN valid = 0 THEN 1  ELSE 0 END) AS incorrect, SUM(CASE valid WHEN 1 THEN 1 ELSE -1 END) AS score, MAX(time_placed) AS last_active FROM chain_info WHERE user_id IS NOT NULL GROUP BY server_id, user_id
    ) x
SET server_user_info.correct = x.correct, server_user_info.incorrect = x.incorrect, server_user_info.score = x.score, server_user_info.last_active = x.last_active WHERE server_user_info.server_id = x.server_id AND server_user_info.user_id = x.user_id;

-- update global user info
UPDATE global_user_info,
    (
        SELECT user_id, SUM(CASE WHEN valid = 1 THEN 1 ELSE 0 END) AS correct, SUM(CASE WHEN valid = 0 THEN 1  ELSE 0 END) AS incorrect, SUM(CASE valid WHEN 1 THEN 1 ELSE -1 END) AS score, MAX(time_placed) AS last_active FROM chain_info WHERE user_id IS NOT NULL GROUP BY user_id
    ) x
SET global_user_info.correct = x.correct, global_user_info.incorrect = x.incorrect, global_user_info.score = x.score, global_user_info.last_active = x.last_active WHERE global_user_info.user_id = x.user_id;

-- leaderboard eligible
ALTER TABLE server_info ADD IF NOT EXISTS leaderboard_eligible bool DEFAULT 1;
-- get leaderboard eligibility, update
UPDATE server_info,
    (
        SELECT chain_info.server_id, chain_info.leaderboard_eligible FROM chain_info, 
            (
                SELECT chain_info.server_id, chain_info.round_number, MAX(chain_info.count) as c FROM chain_info, 
                (
                    SELECT server_id, MAX(round_number) AS r FROM chain_info GROUP BY server_id
                ) x
                WHERE chain_info.server_id = x.server_id AND chain_info.round_number = x.r GROUP BY server_id
            ) y 
        WHERE chain_info.server_id = y.server_id AND chain_info.round_number = y.round_number AND chain_info.count = y.c
    ) z
SET server_info.leaderboard_eligible = z.leaderboard_eligible WHERE server_info.server_id = z.server_id;

-- emoji field
ALTER TABLE server_info ADD IF NOT EXISTS emoji bool DEFAULT 1;

-- convert columns to timestamps
ALTER TABLE server_info RENAME COLUMN last_best TO last_best_unix;
ALTER TABLE server_info ADD IF NOT EXISTS last_best TIMESTAMP(6) DEFAULT NULL;
UPDATE server_info SET last_best = FROM_UNIXTIME(last_best_unix);
ALTER TABLE server_info MODIFY last_best TIMESTAMP(6) AFTER last_best_unix;
ALTER TABLE server_info DROP IF EXISTS last_best_unix;

SET FOREIGN_KEY_CHECKS = 0;
ALTER TABLE chain_info DROP PRIMARY KEY;
ALTER TABLE chain_info RENAME COLUMN time_placed TO time_placed_unix;
ALTER TABLE chain_info ADD IF NOT EXISTS time_placed TIMESTAMP(6) DEFAULT NULL;
UPDATE chain_info SET time_placed = FROM_UNIXTIME(time_placed_unix);
ALTER TABLE chain_info MODIFY time_placed TIMESTAMP(6) AFTER time_placed_unix;
ALTER TABLE chain_info ADD CONSTRAINT PRIMARY_KEY PRIMARY KEY (server_id, city_id, round_number, count, time_placed, message_id);
ALTER TABLE chain_info DROP IF EXISTS time_placed_unix;
SET FOREIGN_KEY_CHECKS = 1;

ALTER TABLE server_user_info RENAME COLUMN block_expiry TO block_expiry_unix;
ALTER TABLE server_user_info ADD IF NOT EXISTS block_expiry TIMESTAMP(6) DEFAULT NULL;
UPDATE server_user_info SET block_expiry = FROM_UNIXTIME(block_expiry_unix);
ALTER TABLE server_user_info MODIFY block_expiry TIMESTAMP(6) AFTER block_expiry_unix;
ALTER TABLE server_user_info DROP IF EXISTS block_expiry_unix;

ALTER TABLE server_user_info RENAME COLUMN last_active TO last_active_unix;
ALTER TABLE server_user_info ADD IF NOT EXISTS last_active TIMESTAMP(6) DEFAULT NULL;
UPDATE server_user_info SET last_active = FROM_UNIXTIME(last_active_unix);
ALTER TABLE server_user_info MODIFY last_active TIMESTAMP(6) AFTER last_active_unix;
ALTER TABLE server_user_info DROP IF EXISTS last_active_unix;

ALTER TABLE global_user_info RENAME COLUMN block_expiry TO block_expiry_unix;
ALTER TABLE global_user_info ADD IF NOT EXISTS block_expiry TIMESTAMP(6) DEFAULT NULL;
UPDATE global_user_info SET block_expiry = FROM_UNIXTIME(block_expiry_unix);
ALTER TABLE global_user_info MODIFY block_expiry TIMESTAMP(6) AFTER block_expiry_unix;
ALTER TABLE global_user_info DROP IF EXISTS block_expiry_unix;

ALTER TABLE global_user_info RENAME COLUMN last_active TO last_active_unix;
ALTER TABLE global_user_info ADD IF NOT EXISTS last_active TIMESTAMP(6) DEFAULT NULL;
UPDATE global_user_info SET last_active = FROM_UNIXTIME(last_active_unix);
ALTER TABLE global_user_info MODIFY last_active TIMESTAMP(6) AFTER last_active_unix;
ALTER TABLE global_user_info DROP IF EXISTS last_active_unix;

-- last messages
-- make new column in server_info
ALTER TABLE server_info ADD IF NOT EXISTS last_message BIGINT DEFAULT 0;
UPDATE server_info, 
(
    SELECT * FROM (
        SELECT server_id, round_number, count, message_id, ROW_NUMBER() OVER (PARTITION BY server_id ORDER BY round_number DESC, count DESC) AS rank FROM chain_info
    ) ranks WHERE rank = 1
) last_messages 
SET server_info.last_message = last_messages.message_id WHERE server_info.server_id = last_messages.server_id;

