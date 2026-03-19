/**
 * work_finish — Complete a task (DEV done, QA pass/fail/refine/blocked, architect done/blocked).
 *
 * Delegates side-effects to pipeline service: label transition, state update,
 * issue close/reopen, notifications, and audit logging.
 *
 * All roles (including architect) use the standard pipeline via executeCompletion.
 * Architect workflow: Researching → Done (done, closes issue), Researching → Refining (blocked).
 */
import { jsonResult } from "openclaw/plugin-sdk";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import type { ToolContext } from "../../types.js";
import type { PluginContext, RunCommand } from "../../context.js";
import type { IssueProvider, PrStatus } from "../../providers/provider.js";
import { getRoleWorker, resolveRepoPath } from "../../projects/index.js";
import { executeCompletion, getRule } from "../../services/pipeline.js";
import { log as auditLog } from "../../audit.js";
import { DATA_DIR } from "../../setup/migrate-layout.js";
import { requireWorkspaceDir, resolveChannelId, resolveProject, resolveProvider } from "../helpers.js";
import { getAllRoleIds, isValidResult, getCompletionResults } from "../../roles/index.js";
import { loadWorkflow } from "../../workflow/index.js";

/**
 * Get the current git branch name.
 */
async function getCurrentBranch(repoPath: string, runCommand: RunCommand): Promise<string> {
  const result = await runCommand(["git", "branch", "--show-current"], {
    timeoutMs: 5_000,
    cwd: repoPath,
  });
  return result.stdout.trim();
}

type ReviewAuditContext = {
  feedbackPrUrl?: string;
  isConflictResolutionCycle: boolean;
};

const FEEDBACK_PR_REASONS = new Set([
  "changes_requested",
  "pr_comments",
  "merge_conflict",
  "merge_failed",
]);

async function readReviewAuditContext(
  workspaceDir: string,
  issueId: number,
): Promise<ReviewAuditContext> {
  const auditPath = join(workspaceDir, DATA_DIR, "log", "audit.log");
  let feedbackPrUrl: string | undefined;
  let isConflictResolutionCycle = false;

  try {
    const content = await readFile(auditPath, "utf-8");
    const lines = content.split("\n").filter(Boolean);

    for (let i = lines.length - 1; i >= 0; i--) {
      try {
        const entry = JSON.parse(lines[i]!);
        if (entry.issueId !== issueId || entry.event !== "review_transition") {
          continue;
        }

        if (entry.reason === "merge_conflict") {
          isConflictResolutionCycle = true;
        }

        if (
          !feedbackPrUrl &&
          FEEDBACK_PR_REASONS.has(String(entry.reason)) &&
          typeof entry.prUrl === "string" &&
          entry.prUrl.length > 0
        ) {
          feedbackPrUrl = entry.prUrl;
        }

        if (feedbackPrUrl && isConflictResolutionCycle) {
          break;
        }
      } catch {
        // skip malformed lines
      }
    }
  } catch {
    // If we can't read the audit log, fail open.
  }

  return { feedbackPrUrl, isConflictResolutionCycle };
}

export async function resolveDeveloperPrStatus(
  issueId: number,
  provider: IssueProvider,
  workspaceDir: string,
  explicitPrUrl?: string,
): Promise<{
  prStatus: PrStatus;
  checkedFallbackPrUrl?: string;
  fallbackSource?: "explicit" | "audit_feedback";
  isConflictCycle: boolean;
}> {
  const auditContext = await readReviewAuditContext(workspaceDir, issueId);
  const prStatus = await provider.getPrStatus(issueId);
  if (prStatus.url) {
    return { prStatus, isConflictCycle: auditContext.isConflictResolutionCycle };
  }

  const checkedFallbackPrUrl = explicitPrUrl ?? auditContext.feedbackPrUrl;
  if (!checkedFallbackPrUrl) {
    return {
      prStatus,
      checkedFallbackPrUrl,
      isConflictCycle: auditContext.isConflictResolutionCycle,
    };
  }

  const fallbackPrStatus = await provider.getPrStatusByUrl(checkedFallbackPrUrl);
  if (!fallbackPrStatus?.url) {
    return {
      prStatus,
      checkedFallbackPrUrl,
      isConflictCycle: auditContext.isConflictResolutionCycle,
    };
  }

  return {
    prStatus: fallbackPrStatus,
    checkedFallbackPrUrl,
    fallbackSource: explicitPrUrl ? "explicit" : "audit_feedback",
    isConflictCycle: auditContext.isConflictResolutionCycle,
  };
}

/**
 * Validate that a developer has created a PR for their work.
 * Throws an error if no open (or merged) PR is found for the issue.
 *
 * How getPrStatus signals "no PR":
 *   - Returns `{ url: null }` when no open or merged PR is linked to the issue.
 *   - `url` is non-null for every found PR (open, approved, merged, etc.).
 *   - We check `url === null` rather than the state field to be explicit:
 *     a null URL unambiguously means "nothing found", regardless of state label.
 */
async function validatePrExistsForDeveloper(
  issueId: number,
  repoPath: string,
  provider: IssueProvider,
  runCommand: RunCommand,
  workspaceDir: string,
  projectSlug: string,
  baseBranch: string,
  explicitPrUrl?: string,
): Promise<void> {
  try {
    const {
      prStatus,
      checkedFallbackPrUrl,
      fallbackSource,
      isConflictCycle,
    } = await resolveDeveloperPrStatus(issueId, provider, workspaceDir, explicitPrUrl);

    // url is null when getPrStatus found no open or merged PR for this issue.
    // This covers both "no PR ever created" and "PR was closed without merging".
    if (!prStatus.url) {
      // Best-effort branch hint for a helpful create-PR example.
      let branchHint = "current-branch";
      try {
        branchHint = await getCurrentBranch(repoPath, runCommand);
      } catch {
        // Fall back to generic placeholder
      }

      const fallbackLine = checkedFallbackPrUrl
        ? `✗ Checked review-cycle PR URL: ${checkedFallbackPrUrl}\n`
        : "";

      throw new Error(
        `Cannot mark work_finish(done) without an open PR.\n\n` +
        `✗ No PR linked to issue #${issueId}\n` +
        fallbackLine +
        `✗ Branch hint: ${branchHint}\n\n` +
        `Please create a PR first:\n` +
        `  gh pr create --base ${baseBranch} --head ${branchHint} --title "..." --body "..."\n\n` +
        `If you are updating an existing review-cycle PR, pass its URL via work_finish({ ..., prUrl: "..." }).\n\n` +
        `Then call work_finish again.`,
      );
    }

    // url is set — an open or merged PR exists, either via the issue-linked
    // lookup or the review-cycle URL fallback.
    if (fallbackSource && checkedFallbackPrUrl) {
      await auditLog(workspaceDir, "pr_validation_fallback", {
        project: projectSlug,
        issue: issueId,
        source: fallbackSource,
        prUrl: checkedFallbackPrUrl,
      });
    }

    // Mark PR as "seen" (with eyes emoji) if not already marked.
    // This helps distinguish system-created PRs from human responses.
    // Best-effort — don't block completion if this fails.
    try {
      const hasEyes = await provider.prHasReaction(issueId, "eyes");
      if (!hasEyes) {
        await provider.reactToPr(issueId, "eyes");
      }
    } catch {
      // Ignore errors — marking is cosmetic
    }

    // Conflict resolution validation: When an issue returns from "To Improve" due to
    // merge conflicts, we must verify the PR is actually mergeable before accepting
    // work_finish(done). Without this check, developers can claim success after local
    // rebase but before pushing, causing infinite dispatch loops (#482).
    if (isConflictCycle && prStatus.mergeable === false) {
      await auditLog(workspaceDir, "work_finish_rejected", {
        project: projectSlug,
        issue: issueId,
        reason: "pr_still_conflicting",
        prUrl: prStatus.url,
      });

      const branchName = prStatus.sourceBranch || "your-branch";
      throw new Error(
        `Cannot complete work_finish(done) while PR still shows merge conflicts.\n\n` +
        `✗ PR status: CONFLICTING\n` +
        `✗ PR URL: ${prStatus.url}\n` +
        `✗ Branch: ${branchName}\n\n` +
        `Your local rebase may have succeeded, but changes must be pushed to the remote.\n\n` +
        `Verify your changes were pushed:\n` +
        `  git log origin/${branchName}..HEAD\n` +
        `  # Should show no commits (meaning everything is pushed)\n\n` +
        `If unpushed commits exist, push them:\n` +
        `  git push --force-with-lease origin ${branchName}\n\n` +
        `Wait a few seconds for GitHub to update, then verify the PR:\n` +
        `  gh pr view ${issueId}\n` +
        `  # Should show "Mergeable" status\n\n` +
        `Once the PR shows as mergeable on GitHub, call work_finish again.`,
      );
    }

    if (isConflictCycle) {
      await auditLog(workspaceDir, "conflict_resolution_verified", {
        project: projectSlug,
        issue: issueId,
        prUrl: prStatus.url,
        mergeable: prStatus.mergeable,
      });
    }
  } catch (err) {
    // Re-throw our own validation errors; swallow provider/network errors.
    // Swallowing keeps work_finish unblocked when the API is unreachable.
    if (err instanceof Error && (err.message.startsWith("Cannot mark work_finish(done)") || err.message.startsWith("Cannot complete work_finish(done)"))) {
      throw err;
    }
    console.warn(`PR validation warning for issue #${issueId}:`, err);
  }
}

export function createWorkFinishTool(ctx: PluginContext) {
  return (toolCtx: ToolContext) => ({
    name: "work_finish",
    label: "Work Finish",
    description: `Complete a task: Developer done (PR created, goes to review) or blocked. Tester pass/fail/refine/blocked. Reviewer approve/reject/blocked. Architect done/blocked. Handles label transition, state update, issue close/reopen, notifications, and audit logging.`,
    parameters: {
      type: "object",
      required: ["channelId", "role", "result"],
      properties: {
        channelId: { type: "string", description: "YOUR chat/group ID — the numeric ID of the chat you are in right now (e.g. '-1003844794417'). Do NOT guess; use the ID of the conversation this message came from." },
        role: { type: "string", enum: getAllRoleIds(), description: "Worker role" },
        result: { type: "string", enum: ["done", "pass", "fail", "refine", "blocked", "approve", "reject"], description: "Completion result" },
        summary: { type: "string", description: "Brief summary" },
        prUrl: { type: "string", description: "PR/MR URL (auto-detected if omitted)" },
        createdTasks: {
          type: "array",
          items: {
            type: "object",
            required: ["id", "title", "url"],
            properties: {
              id: { type: "number", description: "Issue ID" },
              title: { type: "string", description: "Issue title" },
              url: { type: "string", description: "Issue URL" },
            },
          },
          description: "Tasks created during this work session (architect creates implementation tasks).",
        },
      },
    },

    async execute(_id: string, params: Record<string, unknown>) {
      const role = params.role as string;
      const result = params.result as string;
      const channelId = resolveChannelId(toolCtx, params.channelId as string | undefined);
      const summary = params.summary as string | undefined;
      const prUrl = params.prUrl as string | undefined;
      const createdTasks = params.createdTasks as Array<{ id: number; title: string; url: string }> | undefined;
      const workspaceDir = requireWorkspaceDir(toolCtx);

      // Validate role:result using registry
      if (!isValidResult(role, result)) {
        const valid = getCompletionResults(role);
        throw new Error(`${role.toUpperCase()} cannot complete with "${result}". Valid results: ${valid.join(", ")}`);
      }

      // Resolve project + worker
      const { project } = await resolveProject(workspaceDir, channelId);
      const roleWorker = getRoleWorker(project, role);

      // Find the first active slot across all levels
      let slotIndex: number | null = null;
      let slotLevel: string | null = null;
      let issueId: number | null = null;

      for (const [level, slots] of Object.entries(roleWorker.levels)) {
        for (let i = 0; i < slots.length; i++) {
          if (slots[i]!.active && slots[i]!.issueId &&
              (!toolCtx.sessionKey || !slots[i]!.sessionKey ||
               slots[i]!.sessionKey === toolCtx.sessionKey)) {
            slotLevel = level;
            slotIndex = i;
            issueId = Number(slots[i]!.issueId);
            break;
          }
        }
        if (issueId !== null) break;
      }

      if (slotIndex === null || slotLevel === null || issueId === null) {
        throw new Error(`${role.toUpperCase()} worker not active on ${project.name}`);
      }

      const { provider } = await resolveProvider(project, ctx.runCommand);
      const workflow = await loadWorkflow(workspaceDir, project.name);

      if (!getRule(role, result, workflow))
        throw new Error(`Invalid completion: ${role}:${result}`);

      const repoPath = resolveRepoPath(project.repo);
      const pluginConfig = ctx.pluginConfig;

      // For developers marking work as done, validate that a PR exists
      if (role === "developer" && result === "done") {
        await validatePrExistsForDeveloper(
          issueId,
          repoPath,
          provider,
          ctx.runCommand,
          workspaceDir,
          project.slug,
          project.baseBranch,
          prUrl,
        );
      }

      const completion = await executeCompletion({
        workspaceDir, projectSlug: project.slug, role, result, issueId, summary, prUrl, provider, repoPath,
        projectName: project.name,
        channels: project.channels,
        pluginConfig,
        level: slotLevel,
        slotIndex,
        runtime: ctx.runtime,
        workflow,
        createdTasks,
        runCommand: ctx.runCommand,
      });

      await auditLog(workspaceDir, "work_finish", {
        project: project.name, issue: issueId, role, result,
        summary: summary ?? null, labelTransition: completion.labelTransition,
      });

      return jsonResult({
        success: true, project: project.name, projectSlug: project.slug, issueId, role, result,
        ...completion,
      });
    },
  });
}
