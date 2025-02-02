import sourceMapSupport from "source-map-support"
sourceMapSupport.install(options)
import path from "path"
import { PerfTimer } from "./util/perf"
import { rimraf } from "rimraf"
import { GlobbyFilterFunction, isGitIgnored } from "globby"
import chalk from "chalk"
import { parseMarkdown } from "./processors/parse"
import { filterContent } from "./processors/filter"
import { emitContent } from "./processors/emit"
import cfg from "../quartz.config"
import { FilePath, FullSlug, joinSegments, slugifyFilePath } from "./util/path"
import chokidar from "chokidar"
import { ProcessedContent } from "./plugins/vfile"
import { Argv, BuildCtx } from "./util/ctx"
import { glob, toPosixPath } from "./util/glob"
import { trace } from "./util/trace"
import { options } from "./util/sourcemap"
import { Mutex } from "async-mutex"
import DepGraph from "./depgraph"
import { getStaticResourcesFromPlugins } from "./plugins"
import fs from "fs"

type Dependencies = Record<string, DepGraph<FilePath> | null>

type BuildData = {
  ctx: BuildCtx
  ignored: GlobbyFilterFunction
  mut: Mutex
  initialSlugs: FullSlug[]
  // TODO merge contentMap and trackedAssets
  contentMap: Map<FilePath, ProcessedContent>
  trackedAssets: Set<FilePath>
  toRebuild: Set<FilePath>
  toRemove: Set<FilePath>
  lastBuildMs: number
  dependencies: Dependencies
}

// Add these constants at the top of the file
const REBUILD_DEBOUNCE_MS = 100 // Prevent multiple rebuilds within 100ms
const CACHE_TTL_MS = 5 * 60 * 1000 // Cache dependency graphs for 5 minutes
const MAX_MEMORY_USAGE_MB = 1024 // 1GB limit
const MAX_CONSECUTIVE_ERRORS = 3
type DependencyCache = {
  timestamp: number
  graph: DepGraph<FilePath> | null
}

const graphCache = new Map<string, DependencyCache>()
let consecutiveErrors = 0
let lastMemoryCheck = Date.now()

type FileEvent = "add" | "change" | "delete"

type DependencyCache = {
  timestamp: number
  graph: DepGraph<FilePath> | null
}

function newBuildId() {
  return Math.random().toString(36).substring(2, 8)
}

async function buildQuartz(argv: Argv, mut: Mutex, clientRefresh: () => void) {
  const ctx: BuildCtx = {
    buildId: newBuildId(),
    argv,
    cfg,
    allSlugs: [],
  }

  const perf = new PerfTimer()
  const output = argv.output

  const pluginCount = Object.values(cfg.plugins).flat().length
  const pluginNames = (key: "transformers" | "filters" | "emitters") =>
    cfg.plugins[key].map((plugin) => plugin.name)
  if (argv.verbose) {
    console.log(`Loaded ${pluginCount} plugins`)
    console.log(`  Transformers: ${pluginNames("transformers").join(", ")}`)
    console.log(`  Filters: ${pluginNames("filters").join(", ")}`)
    console.log(`  Emitters: ${pluginNames("emitters").join(", ")}`)
  }

  const release = await mut.acquire()
  try {
    perf.addEvent("clean")
    await rimraf(path.join(output, "*"), { glob: true })
    console.log(`Cleaned output directory \`${output}\` in ${perf.timeSince("clean")}`)

    perf.addEvent("glob")
    const allFiles = await glob("**/*.*", argv.directory, cfg.configuration.ignorePatterns)
    const fps = allFiles.filter((fp) => fp.endsWith(".md")).sort()
    console.log(
      `Found ${fps.length} input files from \`${argv.directory}\` in ${perf.timeSince("glob")}`,
    )

    const filePaths = fps.map((fp) => joinSegments(argv.directory, fp) as FilePath)
    ctx.allSlugs = allFiles.map((fp) => slugifyFilePath(fp as FilePath))

    const parsedFiles = await parseMarkdown(ctx, filePaths)
    const filteredContent = filterContent(ctx, parsedFiles)

    const dependencies: Record<string, DepGraph<FilePath> | null> = {}

    // Only build dependency graphs if we're doing a fast rebuild
    if (argv.fastRebuild) {
    console.log("Debug - Initializing dependency graphs for fast rebuild")
    const staticResources = getStaticResourcesFromPlugins(ctx)
    
    for (const emitter of cfg.plugins.emitters) {
      const cachedData = graphCache.get(emitter.name)
      const now = Date.now()

      // Use cached graph if it's still valid
      if (cachedData && (now - cachedData.timestamp) < CACHE_TTL_MS) {
        dependencies[emitter.name] = cachedData.graph
        console.log(`Debug - Using cached graph for ${emitter.name}`)
        continue
      }

      // Build new graph and cache it
      console.log(`Debug - Creating dependency graph for emitter: ${emitter.name}`)
      const graph = await emitter.getDependencyGraph?.(ctx, filteredContent, staticResources) ?? null
      dependencies[emitter.name] = graph
      
      graphCache.set(emitter.name, {
        timestamp: now,
        graph
      })
    }
  }

    await emitContent(ctx, filteredContent)
    console.log(chalk.green(`Done processing ${fps.length} files in ${perf.timeSince()}`))

    if (argv.serve) {
      return startServing(ctx, mut, parsedFiles, clientRefresh, dependencies)
    }
  } finally {
    release()
  }
}

// Add debouncing to partialRebuildFromEntrypoint
let rebuildTimeout: NodeJS.Timeout | null = null
let pendingChanges = new Set<{filepath: string, action: FileEvent}>()

async function debouncedRebuild(
  buildData: BuildData,
  clientRefresh: () => void
) {
  if (pendingChanges.size === 0) return

  const changes = [...pendingChanges]
  pendingChanges.clear()

  for (const {filepath, action} of changes) {
    await partialRebuildFromEntrypoint(filepath, action, clientRefresh, buildData)
  }
}

async function queueRebuild(
  filepath: string,
  action: FileEvent,
  clientRefresh: () => void,
  buildData: BuildData
) {
  pendingChanges.add({filepath, action})

  if (rebuildTimeout) {
    clearTimeout(rebuildTimeout)
  }

  rebuildTimeout = setTimeout(() => {
    debouncedRebuild(buildData, clientRefresh)
    rebuildTimeout = null
  }, REBUILD_DEBOUNCE_MS)
}

// setup watcher for rebuilds
async function startServing(
  ctx: BuildCtx,
  mut: Mutex,
  initialContent: ProcessedContent[],
  clientRefresh: () => void,
  dependencies: Dependencies, // emitter name: dep graph
) {
  const { argv } = ctx

  // cache file parse results
  const contentMap = new Map<FilePath, ProcessedContent>()
  for (const content of initialContent) {
    const [_tree, vfile] = content
    contentMap.set(vfile.data.filePath!, content)
  }

  const buildData: BuildData = {
    ctx,
    mut,
    dependencies,
    contentMap,
    ignored: await isGitIgnored(),
    initialSlugs: ctx.allSlugs,
    toRebuild: new Set<FilePath>(),
    toRemove: new Set<FilePath>(),
    trackedAssets: new Set<FilePath>(),
    lastBuildMs: 0,
  }

  const watcher = chokidar.watch(".", {
  persistent: true,
  cwd: argv.directory,
  ignoreInitial: true,
  atomic: true, // Add this line
  awaitWriteFinish: {  // Add this configuration
    stabilityThreshold: 300,
    pollInterval: 100
  }
})

  const buildFromEntry = argv.fastRebuild ? queueRebuild : rebuildFromEntrypoint
  
  watcher
  .on("change", async (fp) => {
    const filePath = joinSegments(argv.directory, toPosixPath(fp)) as FilePath
    const oldContent = contentMap.get(filePath)
    if (oldContent) {
      // Read new content
      const newContent = await fs.promises.readFile(filePath, 'utf8')
      // Get old content from vfile
      const oldFileContent = oldContent[1].value

      // Only rebuild if content actually changed
      if (newContent !== oldFileContent) {
        buildFromEntry(fp as string, "change", clientRefresh, buildData)
      }
    } else {
      buildFromEntry(fp as string, "add", clientRefresh, buildData)
    }
  })
  .on("unlink", (fp) => buildFromEntry(fp as string, "delete", clientRefresh, buildData))

  // Add cleanup for cache
  return async () => {
    if (rebuildTimeout) {
      clearTimeout(rebuildTimeout)
    }
    graphCache.clear()
    await watcher.close()
  }
}

async function partialRebuildFromEntrypoint(
  filepath: string,
  action: FileEvent,
  clientRefresh: () => void,
  buildData: BuildData,
) {
  const { ctx, ignored, dependencies, contentMap, mut, toRemove } = buildData
  const { argv, cfg } = ctx
  let processedFiles: ProcessedContent[] = []
  // Memory check every minute
  if (Date.now() - lastMemoryCheck > 60000) {
    const memoryUsage = process.memoryUsage().heapUsed / 1024 / 1024
    if (memoryUsage > MAX_MEMORY_USAGE_MB) {
      console.log("Debug - Memory usage high, clearing caches")
      graphCache.clear()
      if (global.gc) {
        global.gc()
      }
    }
    lastMemoryCheck = Date.now()
  }

  if (ignored(filepath)) {
    return
  }

  const buildId = newBuildId()
  ctx.buildId = buildId
  buildData.lastBuildMs = new Date().getTime()
  const release = await mut.acquire()

  try {
     // if there's another build after us, release and let them do it
    if (ctx.buildId !== buildId) {
      release()
      return
    }

    const perf = new PerfTimer()
    // UPDATE DEP GRAPH
    const fp = joinSegments(argv.directory, toPosixPath(filepath)) as FilePath
    const staticResources = getStaticResourcesFromPlugins(ctx)
    let processedFiles: ProcessedContent[] = []

    // Use cached dependency graphs if available
    for (const [emitterName, cachedData] of graphCache.entries()) {
      if (Date.now() - cachedData.timestamp < CACHE_TTL_MS) {
        dependencies[emitterName] = cachedData.graph
      } else {
        graphCache.delete(emitterName)
      }
    }

    switch (action) {
      // add to cache when new file is added
      case "add":
        processedFiles = await parseMarkdown(ctx, [fp])
        processedFiles.forEach(([tree, vfile]) => {
          contentMap.set(vfile.data.filePath!, [tree, vfile])
        })

        // update the dep graph by asking all emitters whether they depend on this file
        for (const emitter of cfg.plugins.emitters) {
          const emitterGraph =
            (await emitter.getDependencyGraph?.(ctx, processedFiles, staticResources)) ?? null

          if (emitterGraph) {
            const existingGraph = dependencies[emitter.name]
            if (existingGraph !== null) {
              existingGraph.mergeGraph(emitterGraph)
            } else {
              // might be the first time we're adding a mardown file
              dependencies[emitter.name] = emitterGraph
            }
            
            // Update cache
            graphCache.set(emitter.name, {
              timestamp: Date.now(),
              graph: dependencies[emitter.name]
            })
          }
        }
        break

      case "change":
        // invalidate cache when file is changed
        processedFiles = await parseMarkdown(ctx, [fp])
        processedFiles.forEach(([tree, vfile]) => {
          contentMap.set(vfile.data.filePath!, [tree, vfile])
        })

        // only content files can have added/removed dependencies because of transclusions
        if (path.extname(fp) === ".md") {
          for (const emitter of cfg.plugins.emitters) {
            // get new dependencies from all emitters for this file
            const emitterGraph =
              (await emitter.getDependencyGraph?.(ctx, processedFiles, staticResources)) ?? null

          // only update the graph if the emitter plugin uses the changed file
          // eg. Assets plugin ignores md files, so we skip updating the graph
            if (emitterGraph?.hasNode(fp)) {
              dependencies[emitter.name]?.updateIncomingEdgesForNode(emitterGraph, fp)
              
              // Update cache
              graphCache.set(emitter.name, {
                timestamp: Date.now(),
                graph: dependencies[emitter.name]
              })
            }
          }
        }
        break

      case "delete":
        toRemove.add(fp)
        break
    }

    // EMIT
    perf.addEvent("rebuild")
    let emittedFiles = 0

    // Batch process emitters
    const emitterPromises = cfg.plugins.emitters.map(async (emitter) => {
      const depGraph = dependencies[emitter.name]

      // emitter hasn't defined a dependency graph. call it with all processed files
      if (depGraph === null) {
        const files = [...contentMap.values()].filter(
          ([_node, vfile]) => !toRemove.has(vfile.data.filePath!)
        )
        return await emitter.emit(ctx, files, staticResources)
      }

      // only call the emitter if it uses this file
      if (depGraph.hasNode(fp)) {
        // re-emit using all files that are needed for the downstream of this file
        // eg. for ContentIndex, the dep graph could be:
        // a.md --> contentIndex.json
        // b.md ------^
        //
        // if a.md changes, we need to re-emit contentIndex.json,
        // and supply [a.md, b.md] to the emitter
        const upstreams = [...depGraph.getLeafNodeAncestors(fp)] as FilePath[]
        const upstreamContent = upstreams
        // filter out non-markdown files
          .filter((file) => contentMap.has(file) && !toRemove.has(file))
          .map((file) => contentMap.get(file)!)

        return await emitter.emit(ctx, upstreamContent, staticResources)
      }

      return []
    })

    const emittedFileArrays = await Promise.all(emitterPromises)
    emittedFiles = emittedFileArrays.reduce((sum, arr) => sum + arr.length, 0)

    // Cleanup in batches
    if (toRemove.size > 0) {
      const destinationsToDelete = new Set<FilePath>()
      
      for (const file of toRemove) {
        contentMap.delete(file)
        Object.entries(dependencies).forEach(([name, depGraph]) => {
          if (depGraph?.hasNode(file)) {
            depGraph.removeNode(file)
            const orphanNodes = depGraph.removeOrphanNodes()
            orphanNodes?.forEach((node) => {
              if (node.startsWith(argv.output)) {
                destinationsToDelete.add(node)
              }
            })
          }
        })
      }

      if (destinationsToDelete.size > 0) {
        await rimraf([...destinationsToDelete])
      }
    }

    consecutiveErrors = 0 // Reset error counter on success
    console.log(chalk.green(`Done rebuilding in ${perf.timeSince()}`))
  } catch (error) {
    consecutiveErrors++
    if (consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
      console.log("Debug - Too many consecutive errors, clearing caches")
      graphCache.clear()
      consecutiveErrors = 0
    }
    throw error
  } finally {
    // Cleanup
    processedFiles = []
    toRemove.clear()
    release()
    clientRefresh()
  }
}

async function rebuildFromEntrypoint(
  fp: string,
  action: FileEvent,
  clientRefresh: () => void,
  buildData: BuildData, // note: this function mutates buildData
) {
  const { ctx, ignored, mut, initialSlugs, contentMap, toRebuild, toRemove, trackedAssets } =
    buildData

  const { argv } = ctx

  // don't do anything for gitignored files
  if (ignored(fp)) {
    return
  }

  // dont bother rebuilding for non-content files, just track and refresh
  fp = toPosixPath(fp)
  const filePath = joinSegments(argv.directory, fp) as FilePath
  if (path.extname(fp) !== ".md") {
    if (action === "add" || action === "change") {
      trackedAssets.add(filePath)
    } else if (action === "delete") {
      trackedAssets.delete(filePath)
    }
    clientRefresh()
    return
  }

  if (action === "add" || action === "change") {
    toRebuild.add(filePath)
  } else if (action === "delete") {
    toRemove.add(filePath)
  }

  const buildId = newBuildId()
  ctx.buildId = buildId
  buildData.lastBuildMs = new Date().getTime()
  const release = await mut.acquire()

  // there's another build after us, release and let them do it
  if (ctx.buildId !== buildId) {
    release()
    return
  }

  const perf = new PerfTimer()
  console.log(chalk.yellow("Detected change, rebuilding..."))

  try {
    const filesToRebuild = [...toRebuild].filter((fp) => !toRemove.has(fp))
    const parsedContent = await parseMarkdown(ctx, filesToRebuild)
    for (const content of parsedContent) {
      const [_tree, vfile] = content
      contentMap.set(vfile.data.filePath!, content)
    }

    for (const fp of toRemove) {
      contentMap.delete(fp)
    }

    const parsedFiles = [...contentMap.values()]
    const filteredContent = filterContent(ctx, parsedFiles)

    // re-update slugs
    const trackedSlugs = [...new Set([...contentMap.keys(), ...toRebuild, ...trackedAssets])]
      .filter((fp) => !toRemove.has(fp))
      .map((fp) => slugifyFilePath(path.posix.relative(argv.directory, fp) as FilePath))

    ctx.allSlugs = [...new Set([...initialSlugs, ...trackedSlugs])]

    // TODO: we can probably traverse the link graph to figure out what's safe to delete here
    // instead of just deleting everything
    await rimraf(path.join(argv.output, ".*"), { glob: true })
    await emitContent(ctx, filteredContent)
    console.log(chalk.green(`Done rebuilding in ${perf.timeSince()}`))
  } catch (err) {
    console.log(chalk.yellow(`Rebuild failed. Waiting on a change to fix the error...`))
    if (argv.verbose) {
      console.log(chalk.red(err))
    }
  }

  clientRefresh()
  toRebuild.clear()
  toRemove.clear()
  release()
}

export default async (argv: Argv, mut: Mutex, clientRefresh: () => void) => {
  try {
    return await buildQuartz(argv, mut, clientRefresh)
  } catch (err) {
    trace("\nExiting Quartz due to a fatal error", err as Error)
  }
}
