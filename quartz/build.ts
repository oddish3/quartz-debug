// Imports
import path from 'path';
import fs from 'fs';
import { promises as fsPromises } from 'fs';
import { Mutex } from 'async-mutex';
import chokidar from 'chokidar';
import chalk from 'chalk';
import sourceMapSupport from 'source-map-support';
import debounce from 'lodash/debounce.js';
import { isGitIgnored, GlobbyFilterFunction } from 'globby';
import { PerfTimer } from './util/perf';
import { parseMarkdown } from './processors/parse';
import { filterContent } from './processors/filter';
import { emitContent } from './processors/emit';
import cfg from '../quartz.config';
import { FilePath, FullSlug, joinSegments, slugifyFilePath } from './util/path';
import { ProcessedContent } from './plugins/vfile';
import { Argv, BuildCtx } from './util/ctx';
import { glob, toPosixPath } from './util/glob';
import { trace } from './util/trace';
import { getStaticResourcesFromPlugins } from './plugins';
import DepGraph from './depgraph';

// Source map support
sourceMapSupport.install();

// Constants
const REBUILD_DEBOUNCE_MS = 100; // Prevent multiple rebuilds within 100ms
const CACHE_TTL_MS = 5 * 60 * 1000; // Cache dependency graphs for 5 minutes

// Types
type Dependencies = Record<string, DepGraph<FilePath> | null>;

type BuildData = {
  ctx: BuildCtx;
  ignored: GlobbyFilterFunction;
  contentMap: Map<FilePath, ProcessedContent>;
  trackedAssets: Set<FilePath>;
  dependencies: Dependencies;
};

type FileEvent = 'add' | 'change' | 'delete';

type DependencyCache = {
  timestamp: number;
  graph: DepGraph<FilePath> | null;
};

// Dependency graph cache
const graphCache = new Map<string, DependencyCache>();

// Build ID generator
function generateBuildId(): string {
  return Math.random().toString(36).substring(2, 8);
}

// Main build function
async function buildQuartz(argv: Argv, mutex: Mutex, clientRefresh: () => void): Promise<() => void> {
  const ctx: BuildCtx = {
    buildId: generateBuildId(),
    argv,
    cfg,
    allSlugs: [],
    mutex,
  };

  const perf = new PerfTimer();
  const outputDir = argv.output;

  // Log plugin information
  const pluginCount = Object.values(cfg.plugins).flat().length;
  const pluginNames = (key: 'transformers' | 'filters' | 'emitters') =>
    cfg.plugins[key].map((plugin) => plugin.name);

  if (argv.verbose) {
    console.log(`Loaded ${pluginCount} plugins`);
    console.log(`  Transformers: ${pluginNames('transformers').join(', ')}`);
    console.log(`  Filters: ${pluginNames('filters').join(', ')}`);
    console.log(`  Emitters: ${pluginNames('emitters').join(', ')}`);
  }

  // Acquire mutex to prevent concurrent builds
  await mutex.acquire();

  try {
    // Clean output directory
    perf.addEvent('clean');
    await fsPromises.rm(outputDir, { recursive: true, force: true });
    console.log(`Cleaned output directory \`${outputDir}\` in ${perf.timeSince('clean')}`);

    // Find all markdown files
    perf.addEvent('glob');
    const allFiles = await glob('**/*.*', argv.directory, cfg.configuration.ignorePatterns);
    const markdownFiles = allFiles.filter((fp) => fp.endsWith('.md')).sort();
    console.log(`Found ${markdownFiles.length} input files in ${perf.timeSince('glob')}`);

    const filePaths = markdownFiles.map((fp) => joinSegments(argv.directory, fp) as FilePath);
    ctx.allSlugs = markdownFiles.map((fp) => slugifyFilePath(fp as FilePath));

    // Parse and process markdown files
    const parsedFiles = await parseMarkdown(ctx, filePaths);
    const filteredContent = filterContent(ctx, parsedFiles);

    // Build dependency graphs
    const dependencies: Dependencies = await buildDependencies(ctx, filteredContent, argv.fastRebuild);

    // Emit content
    await emitContent(ctx, filteredContent);
    console.log(chalk.green(`Done processing ${markdownFiles.length} files in ${perf.timeSince()}`));

    // Start serving if requested
    if (argv.serve) {
      return startServing(ctx, parsedFiles, clientRefresh, dependencies);
    }
  } finally {
    mutex.release();
  }
}

// Build dependency graphs (same as previous implementation)
async function buildDependencies(
  ctx: BuildCtx,
  content: ProcessedContent[],
  fastRebuild: boolean,
): Promise<Dependencies> {
  const dependencies: Dependencies = {};

  if (!fastRebuild) {
    return dependencies;
  }

  console.log('Initializing dependency graphs for fast rebuild');
  const staticResources = getStaticResourcesFromPlugins(ctx);

  for (const emitter of ctx.cfg.plugins.emitters) {
    const cachedData = graphCache.get(emitter.name);
    const now = Date.now();

    if (cachedData && now - cachedData.timestamp < CACHE_TTL_MS) {
      dependencies[emitter.name] = cachedData.graph;
      console.log(`Using cached graph for ${emitter.name}`);
      continue;
    }

    console.log(`Creating dependency graph for emitter: ${emitter.name}`);
    const graph = (await emitter.getDependencyGraph?.(ctx, content, staticResources)) ?? null;
    dependencies[emitter.name] = graph;

    graphCache.set(emitter.name, {
      timestamp: now,
      graph,
    });
  }

  return dependencies;
}

// Start file watching and serving
async function startServing(
  ctx: BuildCtx,
  initialContent: ProcessedContent[],
  clientRefresh: () => void,
  dependencies: Dependencies,
): Promise<() => Promise<void>> {
  const { argv, mutex } = ctx;

  // Cache initial parsed content
  const contentMap = new Map<FilePath, ProcessedContent>();
  for (const content of initialContent) {
    const [, vfile] = content;
    contentMap.set(vfile.data.filePath!, content);
  }

  // Obtain the `ignored` function
  const ignored = await isGitIgnored({
    cwd: argv.directory,
  });

  const buildData: BuildData = {
    ctx,
    ignored,
    contentMap,
    trackedAssets: new Set<FilePath>(),
    dependencies,
  };

  // Initialize file watcher
  const watcher = chokidar.watch(argv.directory, {
    persistent: true,
    // Remove `cwd` to use absolute paths
    ignoreInitial: true,
    atomic: true,
    awaitWriteFinish: {
      stabilityThreshold: 300,
      pollInterval: 100,
    },
    ignored: (filePath) => {
      // Ignore dotfiles and node_modules
      return /(^|[/\\])\../.test(filePath) || /node_modules/.test(filePath);
    },
  });

  // Map to store pending changes
  const pendingChanges = new Map<FilePath, FileEvent>();

  // Handle pending file changes
  const handlePendingChanges = async () => {
    await mutex.acquire();

    try {
      const { ctx, contentMap, trackedAssets, dependencies } = buildData;
      const { argv } = ctx;

      const perf = new PerfTimer();
      const staticResources = getStaticResourcesFromPlugins(ctx);

      // Process pending changes
      const changes = Array.from(pendingChanges.entries());
      pendingChanges.clear();

      let requiresFullRefresh = false;

      for (const [filePath, event] of changes) {
        // Convert to relative path from repository root
        const relativeFilePath = path.relative(argv.directory, filePath);

        // Diagnostic logging
        console.log(`Processing ${event} event for file: ${filePath}`);
        console.log(`Relative path: ${relativeFilePath}`);

        // Check if file is ignored
        if (buildData.ignored(relativeFilePath)) {
          console.log(`Ignored file: ${relativeFilePath}`);
          continue;
        }

        if (path.extname(filePath) !== '.md') {
          // Handle non-markdown files (assets)
          if (event === 'add' || event === 'change') {
            trackedAssets.add(filePath);
          } else if (event === 'delete') {
            trackedAssets.delete(filePath);
          }
          continue;
        }

        switch (event) {
          case 'add':
          case 'change':
            // Parse and update content map
            if (fs.existsSync(filePath)) {
              console.log(chalk.blue(`Processing file: ${relativeFilePath}`));
              const parsedContent = await parseMarkdown(ctx, [filePath]);
              const filteredContent = filterContent(ctx, parsedContent);

              filteredContent.forEach(([tree, vfile]) => {
                contentMap.set(vfile.data.filePath!, [tree, vfile]);
              });

              requiresFullRefresh = true;
            } else {
              console.log(chalk.yellow(`File not found: ${relativeFilePath}`));
            }
            break;

          case 'delete':
            // Remove from content map and delete emitted files
            console.log(chalk.yellow(`Removing file: ${relativeFilePath}`));
            contentMap.delete(filePath);
            requiresFullRefresh = true;

            const slugToRemove = slugifyFilePath(relativeFilePath as FilePath);
            const outputPath = path.join(argv.output, slugToRemove);
            await fsPromises.rm(outputPath, { recursive: true, force: true });
            break;
        }
      }

      if (requiresFullRefresh) {
        // Emit updated content
        const allContent = Array.from(contentMap.values());
        await emitContent(ctx, allContent);
        console.log(chalk.green(`Rebuild completed in ${perf.timeSince()}`));
      }

      // Trigger client refresh
      clientRefresh();
    } catch (error) {
      console.error(chalk.red('Error during rebuild:', error));
    } finally {
      mutex.release();
    }
  };

  // Set up debounced rebuild handler
  const debouncedRebuild = debounce(handlePendingChanges, REBUILD_DEBOUNCE_MS);

  // Handle file events
  const handleFileEvent = (event: FileEvent) => (filePath: string) => {
    // Since `cwd` is not set, `filePath` is absolute
    const absolutePath = path.resolve(filePath);
    pendingChanges.set(absolutePath, event);
    debouncedRebuild();
  };

  // Attach event handlers
  watcher
    .on('add', handleFileEvent('add'))
    .on('change', handleFileEvent('change'))
    .on('unlink', handleFileEvent('delete'));

  // Cleanup function
  return async () => {
    debouncedRebuild.cancel();
    graphCache.clear();
    pendingChanges.clear();
    await watcher.close();
  };
}

// Exported build function
export default async (argv: Argv, mutex: Mutex, clientRefresh: () => void) => {
  try {
    return await buildQuartz(argv, mutex, clientRefresh);
  } catch (err) {
    trace('\nExiting Quartz due to a fatal error', err as Error);
  }
}

