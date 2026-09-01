// Types
export type {
  RepoPath,
  BranchName,
  FullCommitSha,
  WorktreePath,
  GitResult,
  GitError,
  WorkspaceSyncMode,
  WorkspaceSyncState,
  WorkspaceSyncResult,
  WorktreeInfo,
} from './types';
export { toRepoPath, toBranchName, toWorktreePath, isFullCommitSha } from './types';

// Process and filesystem wrappers
export { execFileAsync, mkdirAsync, resolveBashPath } from './exec';

// Worktree operations
export {
  getWorktreeBase,
  isProjectScopedWorktreeBase,
  worktreeExists,
  listWorktrees,
  findWorktreeByBranch,
  isWorktreePath,
  removeWorktree,
  getCanonicalRepoPath,
  verifyWorktreeOwnership,
  verifyWorktreeHead,
} from './worktree';
export type { WorktreeLayout, WorktreeBaseOverride } from './worktree';

// Branch operations
export {
  getDefaultBranch,
  getUniqueCommitCount,
  getCurrentBranch,
  countCommitsAhead,
  checkout,
  hasUncommittedChanges,
  commitAllChanges,
  isBranchMerged,
  isPatchEquivalent,
  isAncestorOf,
  getLastCommitDate,
} from './branch';

// Forge detection
export { detectForge } from './forge';
export type { ForgeType, ForgeInfo } from './forge';

// Repository operations
export {
  findRepoRoot,
  resolveFullCommitSha,
  getDefaultRemote,
  getRemoteUrl,
  listChildRepos,
  syncWorkspace,
  cloneRepository,
  syncRepository,
  addSafeDirectory,
} from './repo';
