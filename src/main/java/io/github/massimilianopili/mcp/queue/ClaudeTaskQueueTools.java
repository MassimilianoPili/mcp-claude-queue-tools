package io.github.massimilianopili.mcp.queue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.massimilianopili.ai.reactive.annotation.ReactiveTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Task queue tools for inter-session Claude coordination.
 * Triple-write: PostgreSQL (source of truth) + Redis (dispatch) + Preference Sort (ranking).
 */
@Service
public class ClaudeTaskQueueTools {

    private static final Logger log = LoggerFactory.getLogger(ClaudeTaskQueueTools.class);

    private final JdbcTemplate jdbc;
    private final ReactiveStringRedisTemplate msg;
    private final QueueProperties props;
    private final HttpClient http = HttpClient.newHttpClient();
    private final ObjectMapper mapper = new ObjectMapper();

    public ClaudeTaskQueueTools(
            @Qualifier("taskQueueDataSource") javax.sql.DataSource dataSource,
            @Qualifier("mcpRedisMessagingTemplate") ReactiveStringRedisTemplate msg,
            QueueProperties props) {
        this.jdbc = new JdbcTemplate(dataSource);
        this.msg = msg;
        this.props = props;
        log.info("ClaudeTaskQueueTools initialized (DB + Redis + Preference Sort)");
    }

    @ReactiveTool(name = "claude_task_enqueue",
            description = "Enqueues a task for a future agent. Dual-write: PostgreSQL (durability) + Redis (dispatch). " +
                    "If targetLabel is null, any future session can dequeue it. " +
                    "Supports dependencies: use dependsOn to specify task IDs that must complete first.")
    public Mono<String> claudeTaskEnqueue(
            @ToolParam(description = "Correlation slug (e.g. 'research-gp', 'deploy-check')") String ref,
            @ToolParam(description = "Task type (e.g. 'research', 'code-review', 'ops', 'test')") String taskType,
            @ToolParam(description = "JSON task payload with fields: task, context, constraints") String payloadJson,
            @ToolParam(description = "Creating session label (e.g. 'chat-31')") String createdBy,
            @ToolParam(description = "Specific target label (null = anyone)", required = false) String targetLabel,
            @ToolParam(description = "Priority 1-10 (1=urgent, 5=default, 10=low)", required = false) Integer priority,
            @ToolParam(description = "Comma-separated task IDs this task depends on (e.g. '3,5,12')", required = false) String dependsOn,
            @ToolParam(description = "Path to plan file (e.g. ~/.claude/plans/foo.md)", required = false) String planFile,
            @ToolParam(description = "Impact 1-10 (1=minimal, 10=critical)", required = false) Integer impact,
            @ToolParam(description = "Effort 1-5 (1=<1h, 2=hours, 3=day, 4=days, 5=week+)", required = false) Integer effort,
            @ToolParam(description = "Confidence 1-5 (how sure about impact/effort estimates)", required = false) Integer confidence,
            @ToolParam(description = "Urgency 1-10 (1=no rush, 10=do now)", required = false) Integer urgency,
            @ToolParam(description = "Deadline ISO date (e.g. '2026-05-01'), null if none", required = false) String deadline,
            @ToolParam(description = "Tier 1-4 (1=must, 2=should, 3=could, 4=wont)", required = false) Integer tier,
            @ToolParam(description = "Category: ops, code, research, project, security, awareness", required = false) String category) {

        int prio = (priority != null) ? Math.max(1, Math.min(10, priority)) : props.getDefaultPriority();
        Integer clampedImpact = clamp(impact, 1, 10);
        Integer clampedEffort = clamp(effort, 1, 5);
        Integer clampedConfidence = clamp(confidence, 1, 5);
        Integer clampedUrgency = clamp(urgency, 1, 10);
        Integer clampedTier = clamp(tier, 1, 4);
        java.sql.Date parsedDeadline = parseDate(deadline);
        List<Long> depIds = parseDependsOn(dependsOn);

        return Mono.fromCallable(() -> {
            Long taskId = jdbc.queryForObject(
                    "INSERT INTO claude_tasks (ref, task_type, payload_json, created_by, target_label, priority, " +
                            "plan_file_path, impact, effort, confidence, urgency, due_date, tier, category) " +
                            "VALUES (?, ?, ?::jsonb, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING task_id",
                    Long.class, ref, taskType, payloadJson, createdBy, targetLabel, prio,
                    planFile, clampedImpact, clampedEffort, clampedConfidence, clampedUrgency,
                    parsedDeadline, clampedTier, category);

            List<String> depWarnings = new ArrayList<>();
            for (Long depId : depIds) {
                try {
                    jdbc.update("INSERT INTO claude_task_deps (task_id, depends_on_id) VALUES (?, ?)", taskId, depId);
                } catch (Exception e) {
                    depWarnings.add("#" + depId + " (" + e.getMessage().split("\n")[0] + ")");
                }
            }

            String redisMsg = String.format(
                    "{\"task_id\":%d,\"ref\":\"%s\",\"priority\":%d,\"type\":\"%s\",\"deps\":%d}",
                    taskId, ref, prio, taskType, depIds.size());
            boolean redisOk = false;
            try {
                msg.opsForList().leftPush(props.getRedisKey(), redisMsg).block();
                jdbc.update("UPDATE claude_tasks SET redis_key = ?, dispatched_at = now() WHERE task_id = ?", props.getRedisKey(), taskId);
                redisOk = true;
            } catch (Exception e) {
                log.warn("Redis dispatch failed for task #{}: {}", taskId, e.getMessage());
            }

            boolean rankOk = false;
            try {
                String listUuid = findOrCreateTaskQueueList();
                addItemToRanking(listUuid, taskId, ref, taskType);
                rankOk = true;
            } catch (Exception e) {
                log.warn("Preference Sort registration failed for task #{}: {}", taskId, e.getMessage());
            }

            String sinks = (redisOk ? "Redis" : "") + (redisOk && rankOk ? " + " : "") + (rankOk ? "Ranker" : "");
            if (sinks.isEmpty()) sinks = "DB only";
            String depStr = depIds.isEmpty() ? "" :
                    String.format(", depends on: %s", depIds.stream().map(id -> "#" + id).collect(Collectors.joining(",")));
            String warnStr = depWarnings.isEmpty() ? "" :
                    String.format("\nWARN deps not found: %s", String.join(", ", depWarnings));
            String planStr = (planFile != null && !planFile.isBlank()) ? ", plan: " + planFile : "";
            return String.format("Task #%d enqueued (DB + %s) — ref: %s, type: %s, priority: %d%s%s%s",
                    taskId, sinks, ref, taskType, prio, planStr, depStr, warnStr);
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @ReactiveTool(name = "claude_task_claim",
            description = "Claims a specific task. Updates PostgreSQL: status=CLAIMED. Returns the task payload. " +
                    "Blocks if the task has unmet dependencies.")
    public Mono<String> claudeTaskClaim(
            @ToolParam(description = "ID of the task to claim") Long taskId,
            @ToolParam(description = "Session label (e.g. 'chat-42')") String claimedBy) {

        return Mono.fromCallable(() -> {
            List<Map<String, Object>> unmetDeps = jdbc.queryForList(
                    "SELECT d.depends_on_id, t.status " +
                            "FROM claude_task_deps d JOIN claude_tasks t ON t.task_id = d.depends_on_id " +
                            "WHERE d.task_id = ? AND t.status <> 'COMPLETED'",
                    taskId);

            if (!unmetDeps.isEmpty()) {
                String depList = unmetDeps.stream()
                        .map(d -> "#" + d.get("depends_on_id") + " (" + d.get("status") + ")")
                        .collect(Collectors.joining(", "));
                return "BLOCKED: Task #" + taskId + " has unmet dependencies: " + depList;
            }

            List<Map<String, Object>> rows = jdbc.queryForList(
                    "UPDATE claude_tasks SET status = 'CLAIMED', claimed_by = ?, claimed_at = now() " +
                            "WHERE task_id = ? AND status = 'PENDING' " +
                            "RETURNING task_id, ref, task_type, payload_json::text, priority, created_by, " +
                            "to_char(created_at, 'YYYY-MM-DD HH24:MI') as created, plan_file_path",
                    claimedBy, taskId);

            if (rows.isEmpty()) {
                return "ERROR: Task #" + taskId + " not found or not PENDING";
            }

            Map<String, Object> row = rows.getFirst();
            String planLine = row.get("plan_file_path") != null ? "\nPlan: " + row.get("plan_file_path") : "";
            return String.format("Task #%s claimed by %s\nRef: %s | Type: %s | Prio: %s | By: %s (%s)\nPayload: %s%s",
                    row.get("task_id"), claimedBy,
                    row.get("ref"), row.get("task_type"), row.get("priority"),
                    row.get("created_by"), row.get("created"),
                    row.get("payload_json"), planLine);
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @ReactiveTool(name = "claude_task_complete",
            description = "Marks a task as completed with its result. " +
                    "Status: 'success' or 'partial' -> COMPLETED, 'failed' -> FAILED.")
    public Mono<String> claudeTaskComplete(
            @ToolParam(description = "Task ID") Long taskId,
            @ToolParam(description = "Status: success, partial, failed") String status,
            @ToolParam(description = "JSON result") String resultJson) {

        String dbStatus = "failed".equalsIgnoreCase(status) ? "FAILED" : "COMPLETED";

        return Mono.fromCallable(() -> {
            int updated = jdbc.update(
                    "UPDATE claude_tasks SET status = ?, result_json = ?::jsonb, completed_at = now() " +
                            "WHERE task_id = ? AND status = 'CLAIMED'",
                    dbStatus, resultJson, taskId);

            if (updated == 0) {
                return "ERROR: Task #" + taskId + " not found or not CLAIMED";
            }

            try { removeItemFromRanking(taskId); }
            catch (Exception e) { log.warn("Ranking removal failed for task #{}: {}", taskId, e.getMessage()); }

            return String.format("Task #%d -> %s (%s)", taskId, dbStatus, status);
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @ReactiveTool(name = "claude_task_set_priority",
            description = "Updates the priority of an existing task. Only PENDING or CLAIMED tasks can be re-prioritized.")
    public Mono<String> claudeTaskSetPriority(
            @ToolParam(description = "Task ID") Long taskId,
            @ToolParam(description = "New priority 1-10 (1=urgent, 5=default, 10=low)") Integer priority) {

        int prio = Math.max(1, Math.min(10, priority));

        return Mono.fromCallable(() -> {
            List<Map<String, Object>> rows = jdbc.queryForList(
                    "UPDATE claude_tasks SET priority = ? WHERE task_id = ? AND status IN ('PENDING','CLAIMED') " +
                            "RETURNING task_id, ref, priority",
                    prio, taskId);

            if (rows.isEmpty()) {
                return "ERROR: Task #" + taskId + " not found or already completed/failed";
            }

            Map<String, Object> row = rows.getFirst();
            return String.format("Task #%s priority → %s (ref: %s)", row.get("task_id"), row.get("priority"), row.get("ref"));
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @ReactiveTool(name = "claude_task_update_plan",
            description = "Updates or sets the plan file path of an existing task. Only PENDING or CLAIMED tasks.")
    public Mono<String> claudeTaskUpdatePlan(
            @ToolParam(description = "Task ID") Long taskId,
            @ToolParam(description = "New plan file path (e.g. ~/.claude/plans/foo.md), or empty string to clear") String planFile) {

        String path = (planFile == null || planFile.isBlank()) ? null : planFile;

        return Mono.fromCallable(() -> {
            List<Map<String, Object>> rows = jdbc.queryForList(
                    "UPDATE claude_tasks SET plan_file_path = ? WHERE task_id = ? AND status IN ('PENDING','CLAIMED') " +
                            "RETURNING task_id, ref, plan_file_path",
                    path, taskId);

            if (rows.isEmpty()) {
                return "ERROR: Task #" + taskId + " not found or already completed/failed";
            }

            Map<String, Object> row = rows.getFirst();
            return path != null
                    ? String.format("Task #%s plan → %s (ref: %s)", row.get("task_id"), row.get("plan_file_path"), row.get("ref"))
                    : String.format("Task #%s plan cleared (ref: %s)", row.get("task_id"), row.get("ref"));
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @ReactiveTool(name = "claude_progress_persist",
            description = "Saves a progress snapshot in claude_tasks as task_type='progress'. " +
                    "Useful for cross-session continuity.")
    public Mono<String> claudeProgressPersist(
            @ToolParam(description = "Work context title") String title,
            @ToolParam(description = "JSON with current state: {todos: [{content, status}], notes: '...'}") String progressJson,
            @ToolParam(description = "Session label (e.g. 'chat-74')") String createdBy) {

        return Mono.fromCallable(() -> {
            String ref = "progress-" + System.currentTimeMillis() / 1000;
            String payload = String.format("{\"title\":\"%s\",\"progress\":%s}",
                    title.replace("\"", "\\\""), progressJson);

            Long taskId = jdbc.queryForObject(
                    "INSERT INTO claude_tasks (ref, task_type, payload_json, created_by, priority) " +
                            "VALUES (?, 'progress', ?::jsonb, ?, ?) RETURNING task_id",
                    Long.class, ref, payload, createdBy, props.getProgressPriority());

            return String.format("Progress saved as task #%d (ref: %s). Next session: claude_task_list('ALL').",
                    taskId, ref);
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @ReactiveTool(name = "claude_task_list",
            description = "Lists tasks by status from PostgreSQL. " +
                    "Status: PENDING (default), CLAIMED, COMPLETED, FAILED, DISPATCHED, RANKED, ALL.")
    public Mono<List<String>> claudeTaskList(
            @ToolParam(description = "Filter: PENDING, CLAIMED, COMPLETED, FAILED, DISPATCHED, RANKED, ALL", required = false) String status,
            @ToolParam(description = "Max results (default 20, max 500)", required = false) Integer limit,
            @ToolParam(description = "Rows to skip for pagination (default 0)", required = false) Integer offset,
            @ToolParam(description = "Max tier to show (1=must only, 2=must+should, default=all)", required = false) Integer tier,
            @ToolParam(description = "Filter by category: ops, code, research, project, security, awareness", required = false) String category) {

        String filter = (status != null) ? status.toUpperCase() : "PENDING";

        if ("RANKED".equals(filter)) return claudeTaskListRanked();

        int maxRows = (limit != null && limit > 0) ? Math.min(limit, props.getMaxLimit()) : props.getDefaultLimit();
        int skip = (offset != null && offset >= 0) ? offset : 0;

        // Build WHERE clause with parameterized values for user inputs
        StringBuilder where = new StringBuilder(switch (filter) {
            case "DISPATCHED" -> "dispatched_at IS NOT NULL AND status NOT IN ('COMPLETED','FAILED','CANCELLED')";
            case "ALL" -> "TRUE";
            default -> "status = '" + filter + "'";
        });
        List<Object> whereParams = new ArrayList<>();
        if (tier != null && tier >= 1 && tier <= 4) {
            where.append(" AND t.tier <= ?");
            whereParams.add(tier);
        }
        if (category != null && !category.isBlank()) {
            where.append(" AND t.category = ?");
            whereParams.add(category);
        }

        String whereStr = where.toString();

        return Mono.fromCallable(() -> {
            int totalCount = jdbc.queryForObject(
                    "SELECT count(*) FROM claude_tasks t WHERE " + whereStr,
                    Integer.class, whereParams.toArray());

            List<Object> queryParams = new ArrayList<>(whereParams);
            queryParams.add(maxRows);
            queryParams.add(skip);

            List<Map<String, Object>> rows = jdbc.queryForList(
                    "SELECT t.task_id, t.ref, t.task_type, t.priority, t.status, t.created_by, " +
                            "coalesce(t.claimed_by, '') as claimed_by, " +
                            "to_char(t.created_at, 'MM-DD HH24:MI') as created, " +
                            "CASE WHEN t.redis_key IS NOT NULL THEN 'Y' ELSE 'N' END as redis, " +
                            "CASE WHEN t.plan_file_path IS NOT NULL THEN 'Y' ELSE 'N' END as has_plan, " +
                            "left(t.payload_json::text, 200) as payload_preview, " +
                            "t.impact, t.urgency, t.effort, t.confidence, t.tier, t.category, " +
                            "t.static_score, t.bt_score, t.bt_comparisons, " +
                            "ag_catalog.task_final_score(t.static_score, t.bt_score, t.bt_comparisons, t.due_date, t.created_at) as final_score, " +
                            "coalesce(array_agg(d.depends_on_id) FILTER (WHERE d.depends_on_id IS NOT NULL), '{}') as deps " +
                            "FROM claude_tasks t " +
                            "LEFT JOIN claude_task_deps d ON d.task_id = t.task_id " +
                            "WHERE " + whereStr +
                            " GROUP BY t.task_id " +
                            "ORDER BY ag_catalog.task_final_score(t.static_score, t.bt_score, t.bt_comparisons, t.due_date, t.created_at) DESC" +
                            " LIMIT ? OFFSET ?",
                    queryParams.toArray());

            Map<Long, String> taskStatuses = new HashMap<>();
            jdbc.queryForList("SELECT task_id, status FROM claude_tasks")
                    .forEach(s -> taskStatuses.put(((Number) s.get("task_id")).longValue(), s.get("status").toString()));

            List<String> result = new ArrayList<>();
            String header = (skip > 0 || rows.size() < totalCount)
                    ? String.format("=== %s [%d-%d of %d] ===", filter, skip + 1, skip + rows.size(), totalCount)
                    : String.format("=== %d tasks %s ===", totalCount, filter);
            result.add(header);

            for (Map<String, Object> r : rows) {
                Long[] deps = pgArrayToLongArray(r.get("deps"));
                String depStr = deps.length == 0 ? "dep:-" :
                        "dep:" + Arrays.stream(deps).map(id -> "#" + id).collect(Collectors.joining(","));

                String statusVal = r.get("status").toString();
                boolean blocked = "PENDING".equals(statusVal) && deps.length > 0 &&
                        Arrays.stream(deps).anyMatch(id -> !"COMPLETED".equals(taskStatuses.getOrDefault(id, "UNKNOWN")));

                String planFlag = "Y".equals(r.get("has_plan")) ? " [plan]" : "";
                Object fs = r.get("final_score");
                String scoreStr = fs != null ? String.format("%.2f", ((Number) fs).doubleValue()) : "-";
                String dimStr = formatDimensions(r);

                result.add(String.format("#%-4s  %-20s [%-8s]  %s  score:%-5s  %s%s%s  by:%-10s  claimed:%-10s  %s  redis:%s\n       %s",
                        r.get("task_id"), r.get("ref"), r.get("task_type"),
                        dimStr, scoreStr,
                        statusVal, blocked ? " [BLOCKED]" : "", planFlag,
                        r.get("created_by"), r.get("claimed_by"),
                        depStr, r.get("redis"),
                        r.get("payload_preview")));
            }

            if (rows.isEmpty()) result.add("(no " + filter + " tasks)");
            return result;
        }).subscribeOn(Schedulers.boundedElastic());
    }

    // ── Dimensions ─────────────────────────────────────────

    @ReactiveTool(name = "claude_task_set_dimensions",
            description = "Update dimensional scores of an existing task. Each param is optional — only provided values are updated. " +
                    "Only PENDING or CLAIMED tasks can be updated.")
    public Mono<String> claudeTaskSetDimensions(
            @ToolParam(description = "Task ID") Long taskId,
            @ToolParam(description = "Impact 1-10 (1=minimal, 10=critical)", required = false) Integer impact,
            @ToolParam(description = "Effort 1-5 (1=<1h, 2=hours, 3=day, 4=days, 5=week+)", required = false) Integer effort,
            @ToolParam(description = "Confidence 1-5 (how sure about impact/effort estimates)", required = false) Integer confidence,
            @ToolParam(description = "Urgency 1-10 (1=no rush, 10=do now)", required = false) Integer urgency,
            @ToolParam(description = "Deadline ISO date (e.g. '2026-05-01'), 'clear' to remove", required = false) String deadline,
            @ToolParam(description = "Tier 1-4 (1=must, 2=should, 3=could, 4=wont)", required = false) Integer tier,
            @ToolParam(description = "Category: ops, code, research, project, security, awareness", required = false) String category,
            @ToolParam(description = "How many other tasks this unblocks", required = false) Integer blocksCount,
            @ToolParam(description = "Bradley-Terry score from Preference Sort", required = false) Double btScore,
            @ToolParam(description = "Number of BT pairwise comparisons", required = false) Integer btComparisons) {

        return Mono.fromCallable(() -> {
            List<String> setClauses = new ArrayList<>();
            List<Object> params = new ArrayList<>();

            // Note: Spring AI MCP deserializes omitted Integer params as 0, not null.
            // Treat 0 as "not provided" since all scales start at 1.
            if (isSet(impact)) { setClauses.add("impact = ?"); params.add(Math.max(1, Math.min(10, impact))); }
            if (isSet(effort)) { setClauses.add("effort = ?"); params.add(Math.max(1, Math.min(5, effort))); }
            if (isSet(confidence)) { setClauses.add("confidence = ?"); params.add(Math.max(1, Math.min(5, confidence))); }
            if (isSet(urgency)) { setClauses.add("urgency = ?"); params.add(Math.max(1, Math.min(10, urgency))); }
            if (deadline != null && !deadline.isEmpty()) {
                if ("clear".equalsIgnoreCase(deadline)) {
                    setClauses.add("due_date = NULL");
                } else {
                    setClauses.add("due_date = ?::date"); params.add(deadline);
                }
            }
            if (isSet(tier)) { setClauses.add("tier = ?"); params.add(Math.max(1, Math.min(4, tier))); }
            if (category != null && !category.isEmpty()) { setClauses.add("category = ?"); params.add(category); }
            if (blocksCount != null && blocksCount >= 0) { setClauses.add("blocks_count = ?"); params.add(blocksCount); }
            if (btScore != null) { setClauses.add("bt_score = ?"); params.add(btScore); }
            if (isSet(btComparisons)) { setClauses.add("bt_comparisons = ?"); params.add(btComparisons); }

            if (setClauses.isEmpty()) return "ERROR: No dimensions provided to update";

            params.add(taskId);
            String sql = "UPDATE claude_tasks SET " + String.join(", ", setClauses) +
                    " WHERE task_id = ? AND status IN ('PENDING','CLAIMED') RETURNING task_id, ref, static_score";

            List<Map<String, Object>> rows = jdbc.queryForList(sql, params.toArray());
            if (rows.isEmpty()) return "ERROR: Task #" + taskId + " not found or already completed/failed";

            Map<String, Object> row = rows.getFirst();
            Object ss = row.get("static_score");
            String scoreStr = ss != null ? String.format("%.3f", ((Number) ss).doubleValue()) : "-";
            return String.format("Task #%s dimensions updated (ref: %s, static_score: %s)", row.get("task_id"), row.get("ref"), scoreStr);
        }).subscribeOn(Schedulers.boundedElastic());
    }

    // ── Helpers ─────────────────────────────────────────────

    private static boolean isSet(Integer value) {
        return value != null && value != 0;
    }

    private static Integer clamp(Integer value, int min, int max) {
        if (!isSet(value)) return null;
        return Math.max(min, Math.min(max, value));
    }

    private static java.sql.Date parseDate(String dateStr) {
        if (dateStr == null || dateStr.isBlank()) return null;
        try { return java.sql.Date.valueOf(dateStr); }
        catch (IllegalArgumentException e) { return null; }
    }

    private static String formatDimensions(Map<String, Object> r) {
        StringBuilder sb = new StringBuilder();
        Object imp = r.get("impact"), urg = r.get("urgency"), eff = r.get("effort"),
               conf = r.get("confidence"), t = r.get("tier");
        sb.append("I:").append(imp != null ? imp : "-");
        sb.append(" U:").append(urg != null ? urg : "-");
        sb.append(" E:").append(eff != null ? eff : "-");
        sb.append(" C:").append(conf != null ? conf : "-");
        sb.append(" T:").append(t != null ? t : "-");
        return sb.toString();
    }

    private List<Long> parseDependsOn(String dependsOn) {
        if (dependsOn == null || dependsOn.isBlank()) return List.of();
        return Arrays.stream(dependsOn.split(","))
                .map(String::trim).filter(s -> !s.isEmpty())
                .map(Long::parseLong).toList();
    }

    private Long[] pgArrayToLongArray(Object pgArray) {
        if (pgArray == null) return new Long[0];
        if (pgArray instanceof java.sql.Array sqlArray) {
            try {
                Object[] arr = (Object[]) sqlArray.getArray();
                return Arrays.stream(arr).map(o -> ((Number) o).longValue()).toArray(Long[]::new);
            } catch (Exception e) { return new Long[0]; }
        }
        if (pgArray instanceof List<?> list) {
            return list.stream().map(o -> ((Number) o).longValue()).toArray(Long[]::new);
        }
        return new Long[0];
    }

    // ── Preference Sort (best-effort) ──────────────────────

    private String findOrCreateTaskQueueList() throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(props.getPreferenceSortApiUrl() + "/lists?limit=100"))
                .header("X-Auth-User-Id", props.getPreferenceSortUserId()).GET().build();
        JsonNode lists = mapper.readTree(http.send(req, HttpResponse.BodyHandlers.ofString()).body());
        for (JsonNode list : lists) {
            if (props.getTaskQueueCategory().equals(list.path("category").asText())) return list.path("id").asText();
        }
        String body = String.format("{\"name\":\"Task Queue\",\"category\":\"%s\",\"ig_threshold\":%s}",
                props.getTaskQueueCategory(), props.getIgThreshold());
        HttpRequest create = HttpRequest.newBuilder()
                .uri(URI.create(props.getPreferenceSortApiUrl() + "/lists"))
                .header("X-Auth-User-Id", props.getPreferenceSortUserId())
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build();
        JsonNode created = mapper.readTree(http.send(create, HttpResponse.BodyHandlers.ofString()).body());
        return created.path("id").asText();
    }

    private void addItemToRanking(String listUuid, long taskId, String ref, String taskType) throws Exception {
        String itemName = String.format("#%d %s [%s]", taskId, ref, taskType);
        String body = String.format("{\"items\":[{\"name\":\"%s\"}]}", itemName);
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(props.getPreferenceSortApiUrl() + "/lists/" + listUuid + "/items"))
                .header("X-Auth-User-Id", props.getPreferenceSortUserId())
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build();
        JsonNode resp = mapper.readTree(http.send(req, HttpResponse.BodyHandlers.ofString()).body());
        String itemUuid = resp.path(0).path("id").asText(null);
        if (itemUuid != null && !itemUuid.isEmpty()) {
            jdbc.update("UPDATE claude_tasks SET rank_item_uuid = ?::uuid WHERE task_id = ?", itemUuid, taskId);
        }
    }

    private void removeItemFromRanking(long taskId) throws Exception {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "SELECT rank_item_uuid::text FROM claude_tasks WHERE task_id = ? AND rank_item_uuid IS NOT NULL", taskId);
        if (rows.isEmpty()) return;
        String itemUuid = rows.getFirst().get("rank_item_uuid").toString();
        String listUuid = findOrCreateTaskQueueList();
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(props.getPreferenceSortApiUrl() + "/lists/" + listUuid + "/items/" + itemUuid))
                .header("X-Auth-User-Id", props.getPreferenceSortUserId()).DELETE().build();
        http.send(req, HttpResponse.BodyHandlers.ofString());
        jdbc.update("UPDATE claude_tasks SET rank_item_uuid = NULL WHERE task_id = ?", taskId);
    }

    private Mono<List<String>> claudeTaskListRanked() {
        return Mono.fromCallable(() -> {
            List<String> result = new ArrayList<>();
            String listUuid;
            try { listUuid = findOrCreateTaskQueueList(); }
            catch (Exception e) {
                result.add("=== Preference Sort unreachable ===");
                result.add(e.getMessage());
                return result;
            }

            HttpRequest rankReq = HttpRequest.newBuilder()
                    .uri(URI.create(props.getPreferenceSortApiUrl() + "/lists/" + listUuid + "/ranking"))
                    .header("X-Auth-User-Id", props.getPreferenceSortUserId()).GET().build();
            JsonNode ranking = mapper.readTree(http.send(rankReq, HttpResponse.BodyHandlers.ofString()).body());

            boolean converged = ranking.path("converged").asBoolean(false);
            result.add(String.format("=== Tasks PENDING (BT ranking, %s) ===",
                    converged ? "CONVERGED" : "not converged"));

            JsonNode items = ranking.path("items");
            Map<Long, JsonNode> rankMap = new HashMap<>();
            for (JsonNode item : items) {
                String name = item.path("name").asText("");
                if (name.startsWith("#")) {
                    try { rankMap.put(Long.parseLong(name.substring(1).split(" ")[0]), item); }
                    catch (NumberFormatException ignored) {}
                }
            }

            if (rankMap.isEmpty()) { result.add("(no ranked tasks)"); return result; }

            List<Map<String, Object>> rows = jdbc.queryForList(
                    "SELECT task_id, ref, task_type, priority, created_by, " +
                            "to_char(created_at, 'MM-DD HH24:MI') as created, " +
                            "left(payload_json::text, 150) as payload_preview " +
                            "FROM claude_tasks WHERE status = 'PENDING' ORDER BY task_id");

            rows.sort((a, b) -> {
                int aRank = rankMap.containsKey(((Number) a.get("task_id")).longValue())
                        ? rankMap.get(((Number) a.get("task_id")).longValue()).path("rank").asInt(999) : 999;
                int bRank = rankMap.containsKey(((Number) b.get("task_id")).longValue())
                        ? rankMap.get(((Number) b.get("task_id")).longValue()).path("rank").asInt(999) : 999;
                return Integer.compare(aRank, bRank);
            });

            for (Map<String, Object> r : rows) {
                long taskId = ((Number) r.get("task_id")).longValue();
                JsonNode item = rankMap.get(taskId);
                int rank = item != null ? item.path("rank").asInt(0) : 0;
                double score = item != null ? item.path("score").asDouble(1.0) : 1.0;
                double se = item != null ? item.path("se").asDouble(1.0) : 1.0;

                result.add(String.format("rank:%d  #%-4s  %-22s [%-8s]  BT:%.2f  SE:%.2f  by:%-10s  %s\n       %s",
                        rank, r.get("task_id"), r.get("ref"), r.get("task_type"),
                        score, se, r.get("created_by"), r.get("created"),
                        r.get("payload_preview")));
            }
            return result;
        }).subscribeOn(Schedulers.boundedElastic());
    }
}
