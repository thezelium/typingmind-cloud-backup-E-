// TypingMind Cloud Sync & Backup v2.0.0
// Combines features from s3.js and YATSE for comprehensive sync and backup
const EXTENSION_VERSION = "2.0.0";
const EXCLUDED_SETTINGS = [
  "aws-bucket",
  "aws-access-key",
  "aws-secret-key",
  "aws-region",
  "aws-endpoint",
  "encryption-key",
  "chat-sync-metadata",
  "sync-mode",
  "last-cloud-sync",
  "TM_useDraftContent",
  "last-daily-backup",
  "TM_useLastVerifiedToken",
  "TM_useStateUpdateHistory",
  "TM_useGlobalChatLoading",
  "TM_crossTabLastSynced",
  "TM_useLastOpenedChatID",
  "INSTANCE_ID",
];

function getUserDefinedExclusions() {
  const exclusions = localStorage.getItem("sync-exclusions");
  return exclusions ?
    exclusions
    .split(",")
    .map((item) => item.trim())
    .filter((item) => item) : [];
}

function shouldExcludeSetting(key) {
  const userExclusions = getUserDefinedExclusions();
  const isExcluded =
    EXCLUDED_SETTINGS.includes(key) ||
    userExclusions.includes(key) ||
    key.startsWith("CHAT_") ||
    key.startsWith("last-seen-") ||
    key.startsWith("sync-") ||
    !isNaN(key);
  if (isExcluded && userExclusions.includes(key)) {
    logToConsole(
      "debug",
      `Setting excluded by user-defined exclusions: ${key}`
    );
  }
  return isExcluded;
}
let config = {
  syncMode: "disabled",
  syncInterval: 15,
  bucketName: "",
  region: "",
  accessKey: "",
  secretKey: "",
  endpoint: "",
  encryptionKey: "",
};
let isConsoleLoggingEnabled =
  new URLSearchParams(window.location.search).get("log") === "true";
let localMetadata = {
  chats: {},
  settings: {
    items: {},
    lastModified: 0,
    syncedAt: 0,
  },
  lastSyncTime: 0,
};
let persistentDB = null;
let dbConnectionPromise = null;
let dbConnectionRetries = 0;
const MAX_DB_RETRIES = 3;
const DB_RETRY_DELAY = 1000;
const DB_CONNECTION_TIMEOUT = 10000;
let dbHeartbeatInterval = null;
let operationState = {
  isImporting: false,
  isExporting: false,
  isPendingSync: false,
  operationQueue: [],
  isProcessingQueue: false,
  lastSyncStatus: null,
  isCheckingChanges: false,
  lastError: null,
  operationStartTime: null,
  queueProcessingPromise: null,
  completedOperations: new Set(),
  operationTimeouts: new Map(),
};
let backupState = {
  isBackupInProgress: false,
  lastDailyBackup: null,
  lastManualSnapshot: null,
  backupInterval: null,
  isBackupIntervalRunning: false,
};
let lastSeenUpdates = {};
let cloudFileSize = 0;
let localFileSize = 0;
let isLocalDataModified = false;
let pendingSettingsChanges = false;
let activeIntervals = {
  sync: null,
  backup: null,
  changeCheck: null,
};

function clearAllIntervals() {
  if (activeIntervals.sync) {
    clearInterval(activeIntervals.sync);
    activeIntervals.sync = null;
  }
  if (activeIntervals.backup) {
    clearInterval(activeIntervals.backup);
    activeIntervals.backup = null;
  }
  if (activeIntervals.changeCheck) {
    clearInterval(activeIntervals.changeCheck);
    activeIntervals.changeCheck = null;
  }
}
const LOG_ICONS = {
  info: "ℹ️",
  success: "✅",
  warning: "⚠️",
  error: "❌",
  start: "🔄",
  end: "🏁",
  upload: "⬆️",
  download: "⬇️",
  cleanup: "🧹",
  snapshot: "📸",
  encrypt: "🔐",
  decrypt: "🔓",
  progress: "📊",
  time: "⏰",
  wait: "⏳",
  pause: "⏸️",
  resume: "▶️",
  visibility: "👁️",
  active: "📱",
  calendar: "📅",
  tag: "🏷️",
  stop: "🛑",
  skip: "⏩",
};

function logToConsole(type, message, data = null) {
  if (!isConsoleLoggingEnabled) return;
  const timestamp = new Date().toLocaleString();
  const icons = {
    info: "ℹ️",
    success: "✅",
    warning: "⚠️",
    error: "❌",
    start: "🔄",
    end: "🏁",
    upload: "⬆️",
    download: "⬇️",
    cleanup: "🧹",
    snapshot: "📸",
    encrypt: "🔐",
    decrypt: "🔓",
    progress: "📊",
    time: "⏰",
    wait: "⏳",
    pause: "⏸️",
    resume: "▶️",
    visibility: "👁️",
    active: "📱",
    calendar: "📅",
    tag: "🏷️",
    stop: "🛑",
    skip: "⏩",
  };
  const icon = icons[type] || "ℹ️";
  const logMessage = `${icon} [${timestamp}] ${message}`;
  if (/Mobi|Android/i.test(navigator.userAgent)) {
    const container =
      document.getElementById("mobile-log-container") ||
      createMobileLogContainer();
    const logsContent = container.querySelector("#logs-content");
    if (logsContent) {
      const logEntry = document.createElement("div");
      logEntry.className = "text-sm mb-1 break-words";
      logEntry.textContent = logMessage;
      if (data) {
        const dataEntry = document.createElement("div");
        dataEntry.className = "text-xs text-gray-500 ml-4 mb-2";
        dataEntry.textContent = JSON.stringify(data, null, 2);
        logEntry.appendChild(dataEntry);
      }
      const searchContainer = container.querySelector(
        ".flex.items-center.gap-2"
      );
      const searchInput = searchContainer ?
        searchContainer.querySelector("input") :
        null;
      const isSearchActive =
        searchInput && !searchInput.classList.contains("hidden");
      if (isSearchActive) {
        const isReversed =
          container.getAttribute("data-log-reversed") === "true";
        if (isReversed) {
          container.originalLogEntries.unshift(logEntry);
        } else {
          container.originalLogEntries.push(logEntry);
        }
        const query = searchInput.value.trim();
        if (query && logMessage.toLowerCase().includes(query.toLowerCase())) {
          const isReversed =
            container.getAttribute("data-log-reversed") === "true";
          if (isReversed) {
            logsContent.insertBefore(
              logEntry.cloneNode(true),
              logsContent.firstChild
            );
          } else {
            logsContent.appendChild(logEntry.cloneNode(true));
          }
        }
      } else {
        const isReversed =
          container.getAttribute("data-log-reversed") === "true";
        if (isReversed) {
          logsContent.insertBefore(logEntry, logsContent.firstChild);
        } else {
          logsContent.appendChild(logEntry);
        }
      }
    }
  }
  switch (type) {
    case "error":
      console.error(logMessage, data);
      break;
    case "warning":
      console.warn(logMessage, data);
      break;
    default:
      console.log(logMessage, data);
  }
}

function createMobileLogContainer() {
  const container = document.createElement("div");
  container.id = "mobile-log-container";
  container.className =
    "fixed bottom-0 left-0 right-0 bg-black text-white z-[9999]";
  container.setAttribute("data-log-reversed", "false");
  container.style.cssText = `
        height: 200px;
        max-height: 50vh;
        display: ${isConsoleLoggingEnabled ? "block" : "none"};
        resize: vertical;
        overflow-y: auto;
    `;
  const minimizedTag = document.createElement("div");
  minimizedTag.id = "minimized-log-tag";
  minimizedTag.className =
    "fixed bottom-0 right-0 bg-black text-white px-3 py-1 m-2 rounded cursor-pointer z-[9999] hidden";
  minimizedTag.innerHTML = "📋 Show Logs";
  let longPressTimer = null;
  let isDraggingTag = false;
  let tagStartX = 0;
  let tagStartY = 0;
  let tagOffsetX = 0;
  let tagOffsetY = 0;
  const savedPosition = localStorage.getItem("mobile-log-tag-position");
  if (savedPosition) {
    const pos = JSON.parse(savedPosition);
    minimizedTag.style.right = "auto";
    minimizedTag.style.bottom = "auto";
    minimizedTag.style.left = pos.x + "px";
    minimizedTag.style.top = pos.y + "px";
  }

  function startLongPress(e) {
    longPressTimer = setTimeout(() => {
      isDraggingTag = true;
      minimizedTag.style.opacity = "0.7";
      const rect = minimizedTag.getBoundingClientRect();
      const clientX =
        e.type === "touchstart" ? e.touches[0].clientX : e.clientX;
      const clientY =
        e.type === "touchstart" ? e.touches[0].clientY : e.clientY;
      tagOffsetX = clientX - rect.left;
      tagOffsetY = clientY - rect.top;
      document.addEventListener("touchmove", dragTag, {
        passive: false
      });
      document.addEventListener("mousemove", dragTag);
      document.addEventListener("touchend", endDragTag);
      document.addEventListener("mouseup", endDragTag);
    }, 500);
  }

  function stopLongPress() {
    if (longPressTimer) {
      clearTimeout(longPressTimer);
      longPressTimer = null;
    }
  }

  function dragTag(e) {
    if (!isDraggingTag) return;
    e.preventDefault();
    const clientX = e.type === "touchmove" ? e.touches[0].clientX : e.clientX;
    const clientY = e.type === "touchmove" ? e.touches[0].clientY : e.clientY;
    const newX = clientX - tagOffsetX;
    const newY = clientY - tagOffsetY;
    const maxX = window.innerWidth - minimizedTag.offsetWidth;
    const maxY = window.innerHeight - minimizedTag.offsetHeight;
    const constrainedX = Math.max(0, Math.min(newX, maxX));
    const constrainedY = Math.max(0, Math.min(newY, maxY));
    minimizedTag.style.right = "auto";
    minimizedTag.style.bottom = "auto";
    minimizedTag.style.left = constrainedX + "px";
    minimizedTag.style.top = constrainedY + "px";
  }

  function endDragTag() {
    document.removeEventListener("touchmove", dragTag);
    document.removeEventListener("mousemove", dragTag);
    document.removeEventListener("touchend", endDragTag);
    document.removeEventListener("mouseup", endDragTag);
    if (isDraggingTag) {
      isDraggingTag = false;
      minimizedTag.style.opacity = "1";
      const rect = minimizedTag.getBoundingClientRect();
      localStorage.setItem(
        "mobile-log-tag-position",
        JSON.stringify({
          x: rect.left,
          y: rect.top,
        })
      );
    }
    stopLongPress();
  }
  minimizedTag.addEventListener("touchstart", startLongPress);
  minimizedTag.addEventListener("mousedown", startLongPress);
  minimizedTag.addEventListener("touchend", stopLongPress);
  minimizedTag.addEventListener("mouseup", stopLongPress);
  minimizedTag.onclick = (e) => {
    if (!isDraggingTag) {
      container.style.display = "block";
      minimizedTag.style.display = "none";
    }
  };
  document.body.appendChild(minimizedTag);
  const header = document.createElement("div");
  header.className =
    "sticky top-0 left-0 right-0 bg-gray-800 p-2 flex justify-between items-center border-b border-gray-700";
  const searchContainer = document.createElement("div");
  searchContainer.className = "flex items-center gap-2 flex-1 max-w-xs";
  const searchIcon = document.createElement("div");
  searchIcon.className = "text-white text-lg cursor-pointer flex-shrink-0";
  searchIcon.innerHTML = "🔍";
  searchIcon.title = "Search logs";
  const searchInput = document.createElement("input");
  searchInput.type = "text";
  searchInput.placeholder = "Search logs...";
  searchInput.className =
    "hidden bg-gray-700 text-white px-2 py-1 rounded text-sm flex-1 focus:outline-none focus:ring-1 focus:ring-blue-500";
  let isSearchActive = false;
  container.originalLogEntries = [];

  function toggleSearch() {
    if (!isSearchActive) {
      searchInput.classList.remove("hidden");
      searchIcon.innerHTML = "✕";
      searchIcon.title = "Clear search";
      searchInput.focus();
      isSearchActive = true;
      const logsContainer = container.querySelector("#logs-content");
      if (logsContainer) {
        container.originalLogEntries = Array.from(logsContainer.children);
      }
    } else {
      searchInput.classList.add("hidden");
      searchIcon.innerHTML = "🔍";
      searchIcon.title = "Search logs";
      searchInput.value = "";
      isSearchActive = false;
      restoreOriginalLogs();
    }
  }

  function restoreOriginalLogs() {
    const logsContainer = container.querySelector("#logs-content");
    if (logsContainer && container.originalLogEntries.length > 0) {
      logsContainer.innerHTML = "";
      container.originalLogEntries.forEach((entry) =>
        logsContainer.appendChild(entry)
      );
    }
  }

  function performSearch(query) {
    const logsContainer = container.querySelector("#logs-content");
    if (!logsContainer || !container.originalLogEntries.length) return;
    const filteredEntries = container.originalLogEntries.filter((entry) => {
      const text = entry.textContent || "";
      return text.toLowerCase().includes(query.toLowerCase());
    });
    logsContainer.innerHTML = "";
    if (filteredEntries.length === 0) {
      const noResults = document.createElement("div");
      noResults.className = "text-gray-400 text-sm italic p-2";
      noResults.textContent = "No matching logs found";
      logsContainer.appendChild(noResults);
    } else {
      filteredEntries.forEach((entry) => {
        const clonedEntry = entry.cloneNode(true);
        logsContainer.appendChild(clonedEntry);
      });
    }
  }
  searchIcon.addEventListener("click", toggleSearch);
  searchInput.addEventListener("input", (e) => {
    const query = e.target.value.trim();
    if (query === "") {
      restoreOriginalLogs();
    } else {
      performSearch(query);
    }
  });
  searchInput.addEventListener("keydown", (e) => {
    if (e.key === "Escape") {
      toggleSearch();
    }
  });
  searchContainer.appendChild(searchIcon);
  searchContainer.appendChild(searchInput);
  const controls = document.createElement("div");
  controls.className = "flex items-center gap-3";
  const clearBtn = document.createElement("button");
  clearBtn.className = "text-white p-2 hover:bg-gray-700 rounded text-sm";
  clearBtn.textContent = "Clear";
  clearBtn.onclick = () => {
    const logsContainer = container.querySelector("#logs-content");
    if (logsContainer) {
      logsContainer.innerHTML = "";
      container.originalLogEntries = [];
    }
  };
  const exportBtn = document.createElement("button");
  exportBtn.className = "text-white p-2 hover:bg-gray-700 rounded text-sm";
  exportBtn.textContent = "Export";
  exportBtn.addEventListener("click", (e) => {
    e.stopPropagation();
    const logsContainer = container.querySelector("#logs-content");
    if (logsContainer && logsContainer.children.length > 0) {
      const logs = Array.from(logsContainer.children)
        .map((log) => {
          const mainText = log.textContent || "";
          return mainText;
        })
        .join("\n");
      const blob = new Blob([logs], {
        type: "text/plain"
      });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `typingmind-logs-${new Date()
        .toISOString()
        .slice(0, 19)
        .replace(/:/g, "-")}.txt`;
      a.style.display = "none";
      document.body.appendChild(a);
      a.click();
      setTimeout(() => {
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
      }, 100);
      const originalText = exportBtn.textContent;
      exportBtn.textContent = "Done";
      exportBtn.style.backgroundColor = "#10b981";
      setTimeout(() => {
        exportBtn.textContent = originalText;
        exportBtn.style.backgroundColor = "";
      }, 2000);
    }
  });
  let isReversed = false;
  const reverseBtn = document.createElement("button");
  reverseBtn.className = "text-white p-2 hover:bg-gray-700 rounded text-sm";
  reverseBtn.innerHTML = "↕️";
  reverseBtn.title = "Reverse order";
  reverseBtn.addEventListener("click", (e) => {
    e.stopPropagation();
    const logsContainer = container.querySelector("#logs-content");
    if (logsContainer && logsContainer.children.length > 0) {
      const logEntries = Array.from(logsContainer.children);
      logEntries.reverse();
      logsContainer.innerHTML = "";
      logEntries.forEach((entry) => logsContainer.appendChild(entry));
      isReversed = !isReversed;
      container.setAttribute("data-log-reversed", isReversed.toString());
      reverseBtn.innerHTML = isReversed ? "🔽" : "🔼";
      reverseBtn.title = isReversed ?
        "Latest first (click to reverse)" :
        "Latest last (click to reverse)";
    }
  });
  const minimizeBtn = document.createElement("button");
  minimizeBtn.className = "text-white p-2 hover:bg-gray-700 rounded text-sm";
  minimizeBtn.textContent = "—";
  minimizeBtn.onclick = () => {
    container.style.display = "none";
    minimizedTag.style.display = "block";
  };
  const toggleSize = document.createElement("button");
  toggleSize.className = "text-white p-2 hover:bg-gray-700 rounded";
  toggleSize.innerHTML = "□";
  toggleSize.onclick = () => {
    if (container.style.height === "200px") {
      container.style.position = "fixed";
      container.style.top = "0";
      container.style.left = "0";
      container.style.right = "0";
      container.style.bottom = "0";
      container.style.height = "100vh";
      container.style.maxHeight = "100vh";
      container.style.zIndex = "99999";
      logsContent.style.height = "calc(100vh - 36px)";
      toggleSize.innerHTML = "▢";
    } else {
      container.style.position = "fixed";
      container.style.top = "auto";
      container.style.left = "0";
      container.style.right = "0";
      container.style.bottom = "0";
      container.style.height = "200px";
      container.style.maxHeight = "50vh";
      logsContent.style.height = "calc(100% - 36px)";
      toggleSize.innerHTML = "□";
    }
  };
  const closeBtn = document.createElement("button");
  closeBtn.className = "text-white p-2 hover:bg-gray-700 rounded";
  closeBtn.innerHTML = "✕";
  closeBtn.onclick = () => {
    container.style.display = "none";
    minimizedTag.style.display = "none";
    const toggle = document.getElementById("console-logging-toggle");
    if (toggle) toggle.checked = false;
    isConsoleLoggingEnabled = false;
  };
  controls.appendChild(clearBtn);
  controls.appendChild(exportBtn);
  controls.appendChild(reverseBtn);
  controls.appendChild(minimizeBtn);
  controls.appendChild(toggleSize);
  controls.appendChild(closeBtn);
  const dragHandle = document.createElement("div");
  dragHandle.className =
    "absolute -top-1 left-0 right-0 h-1 bg-gray-600 cursor-row-resize";
  dragHandle.style.cursor = "row-resize";
  const logsContent = document.createElement("div");
  logsContent.id = "logs-content";
  logsContent.className = "p-2 overflow-y-auto";
  logsContent.style.height = "calc(100% - 36px)";
  header.appendChild(searchContainer);
  header.appendChild(controls);
  container.appendChild(dragHandle);
  container.appendChild(header);
  container.appendChild(logsContent);
  let startY = 0;
  let startHeight = 0;

  function initDrag(e) {
    startY = e.type === "mousedown" ? e.clientY : e.touches[0].clientY;
    startHeight = parseInt(
      document.defaultView.getComputedStyle(container).height,
      10
    );
    document.documentElement.addEventListener("mousemove", doDrag);
    document.documentElement.addEventListener("mouseup", stopDrag);
    document.documentElement.addEventListener("touchmove", doDrag);
    document.documentElement.addEventListener("touchend", stopDrag);
  }

  function doDrag(e) {
    const currentY = e.type === "mousemove" ? e.clientY : e.touches[0].clientY;
    const newHeight = startHeight - (currentY - startY);
    const minHeight = 100;
    const maxHeight = window.innerHeight * 0.8;
    if (newHeight > minHeight && newHeight < maxHeight) {
      container.style.height = `${newHeight}px`;
    }
  }

  function stopDrag() {
    document.documentElement.removeEventListener("mousemove", doDrag);
    document.documentElement.removeEventListener("mouseup", stopDrag);
    document.documentElement.removeEventListener("touchmove", doDrag);
    document.documentElement.removeEventListener("touchend", stopDrag);
  }
  dragHandle.addEventListener("mousedown", initDrag);
  dragHandle.addEventListener("touchstart", initDrag);
  document.body.appendChild(container);
  return container;
}

function initializeLoggingState() {
  const urlParams = new URLSearchParams(window.location.search);
  const logParam = urlParams.get("log");
  if (logParam === "true") {
    isConsoleLoggingEnabled = true;
    logToConsole(
      "info",
      `TypingMind Cloud Sync & Backup v${EXTENSION_VERSION} initializing...`
    );
  }
}
async function performFullInitialization() {
  try {
    loadConfiguration();
    await loadAwsSdk();
    await loadLocalMetadata();
    await initializeLastSeenUpdates();
    await initializeSettingsMonitoring();
    await setupLocalStorageChangeListener();
    startSyncInterval();
    if (config.syncMode === "sync") {
      await queueOperation("initial-sync", performInitialSync);
    }
    if (config.syncMode !== "disabled") {
      queueOperation(
        "daily-backup-check",
        checkAndPerformDailyBackup,
        [],
        300000
      );
    }
    setupLocalStorageChangeListener();
    monitorIndexedDBForDeletions();
    startPeriodicChangeCheck();
    setupVisibilityChangeHandler();
    try {
      await cleanupMetadataVersions();
      logToConsole(
        "success",
        "Metadata cleanup completed during initialization"
      );
    } catch (cleanupError) {
      logToConsole(
        "warning",
        "Non-critical: Metadata cleanup failed during initialization",
        cleanupError
      );
    }
    logToConsole("success", "Full initialization completed");
    logToConsole("cleanup", "Starting tombstone cleanup...");
    const localCleanupCount = cleanupOldTombstones();
    const cloudCleanupCount = await cleanupCloudTombstones();
    if (localCleanupCount > 0 || cloudCleanupCount > 0) {
      logToConsole("success", "Tombstone cleanup completed", {
        localTombstonesRemoved: localCleanupCount,
        cloudTombstonesRemoved: cloudCleanupCount,
      });
    }
    return true;
  } catch (error) {
    logToConsole("error", "Error during full initialization:", error);
    return false;
  }
}
async function initializeExtension() {
  initializeLoggingState();
  try {
    await loadAwsSdk();
    loadConfiguration();
    insertSyncButton();
    if (!isAwsConfigured()) {
      logToConsole(
        "info",
        "AWS not configured - minimal initialization completed"
      );
      return;
    }
    if (config.syncMode === "disabled") {
      logToConsole(
        "info",
        "Disabled mode - skipping cloud operations initialization"
      );
      return;
    }
    let initialMetadataSaveNeeded = false;
    let settingsMetadataSaveNeeded = false;
    initialMetadataSaveNeeded = await loadLocalMetadata();
    await initializeLastSeenUpdates();
    settingsMetadataSaveNeeded = await initializeSettingsMonitoring();
    try {
      const duplicatesFound = await detectIndexedDBDuplicates();
      if (duplicatesFound) {
        await cleanupIndexedDBDuplicates();
        logToConsole(
          "success",
          "IndexedDB duplicate cleanup completed during extension initialization"
        );
      } else {
        logToConsole("info", "No duplicates found - skipping cleanup");
      }
    } catch (cleanupError) {
      logToConsole(
        "warning",
        "Non-critical: IndexedDB duplicate cleanup failed during extension initialization",
        cleanupError
      );
    }
    let hashesRecalculated = false;
    const allLocalChatsForHash = await getAllChatsFromIndexedDB();
    const localChatsMapForHash = new Map(
      allLocalChatsForHash.map((chat) => [chat.id.replace(/^CHAT_/, ""), chat])
    );
    if (localMetadata.chats) {
      for (const chatId in localMetadata.chats) {
        const cleanChatId = chatId.replace(/^CHAT_/, "");
        const chatData = localChatsMapForHash.get(cleanChatId);
        if (chatData && !localMetadata.chats[chatId].deleted) {
          try {
            const newHash = await generateHash(chatData, "chat");
            if (localMetadata.chats[chatId].hash !== newHash) {
              localMetadata.chats[chatId].hash = newHash;
              hashesRecalculated = true;
            }
          } catch (hashError) {
            logToConsole(
              "error",
              `Error generating hash for chat ${cleanChatId} during init recalc`,
              hashError
            );
          }
        } else if (!chatData && !localMetadata.chats[chatId].deleted) {
          logToConsole(
            "warning",
            `Chat ${cleanChatId} found in metadata but not in IndexedDB during hash recalc.`
          );
        }
      }
    }
    if (
      initialMetadataSaveNeeded ||
      settingsMetadataSaveNeeded ||
      hashesRecalculated
    ) {
      await saveLocalMetadata();
    }
    await setupLocalStorageChangeListener();
    startSyncInterval();

    // Check cloud state FIRST to determine initial action
    if (config.syncMode === "sync") {
      const cloudMetadata = await downloadCloudMetadata(); // Get cloud state early
      const cloudIsEmptyOrNew =
        !cloudMetadata ||
        !cloudMetadata.chats ||
        Object.keys(cloudMetadata.chats).length === 0 ||
        cloudMetadata.lastSyncTime === 0;
      const localHasData =
        localMetadata &&
        localMetadata.chats &&
        Object.keys(localMetadata.chats).length > 0;

      if (cloudIsEmptyOrNew && localHasData) {
        logToConsole(
          "info",
          "Cloud is empty/new but local data exists. Performing initial sync/upload."
        );
        // Use performInitialSync as it handles this scenario.
        await queueOperation(
          "initial-sync-upload",
          performInitialSync,
          [],
          300000
        );
      } else if (!cloudIsEmptyOrNew) {
        logToConsole(
          "info",
          "Cloud data found. Performing standard startup sync check."
        );
        if (document.visibilityState === "visible") {
          queueOperation("startup-sync-check", syncFromCloud, [], 300000);
        }
      } else {
        logToConsole(
          "info",
          "Both cloud and local seem empty or new. No initial sync needed immediately."
        );
        // Normal interval/visibility checks will handle future changes.
      }
    }
    if (config.syncMode !== "disabled") {
      queueOperation(
        "daily-backup-check",
        checkAndPerformDailyBackup,
        [],
        300000
      );
    }
    monitorIndexedDBForDeletions();
    startPeriodicChangeCheck();
    setupVisibilityChangeHandler();
    try {
      await cleanupMetadataVersions();
      logToConsole(
        "success",
        "Metadata cleanup completed during initialization"
      );
    } catch (cleanupError) {
      logToConsole(
        "warning",
        "Non-critical: Metadata cleanup failed during initialization",
        cleanupError
      );
    }
    logToConsole("success", "Full initialization completed");
    logToConsole("cleanup", "Starting tombstone cleanup...");
    const localCleanupCount = cleanupOldTombstones();
    const cloudCleanupCount = await cleanupCloudTombstones();
    if (localCleanupCount > 0 || cloudCleanupCount > 0) {
      logToConsole("success", "Tombstone cleanup completed", {
        localTombstonesRemoved: localCleanupCount,
        cloudTombstonesRemoved: cloudCleanupCount,
      });
    }
    setupVisibilityChangeHandler();
    logToConsole("success", "Initialization completed successfully");
  } catch (error) {
    logToConsole("error", "Error initializing extension:", error);
    throw error;
  }
}
if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", initializeExtension);
} else {
  initializeExtension();
}

function throttle(func, limit) {
  let inThrottle;
  return function(...args) {
    if (!inThrottle) {
      func.apply(this, args);
      inThrottle = true;
      setTimeout(() => (inThrottle = false), limit);
    }
  };
}
async function loadAwsSdk() {
  if (window.AWS) return;
  return new Promise((resolve, reject) => {
    const script = document.createElement("script");
    script.src = "https://sdk.amazonaws.com/js/aws-sdk-2.1048.0.min.js";
    script.onload = () => {
      resolve();
    };
    script.onerror = () => reject(new Error("Failed to load AWS SDK"));
    document.head.appendChild(script);
  });
}
async function loadJSZip() {
  if (window.JSZip) return window.JSZip;
  return new Promise((resolve, reject) => {
    const script = document.createElement("script");
    script.src =
      "https://cdnjs.cloudflare.com/ajax/libs/jszip/3.10.1/jszip.min.js";
    script.onload = () => {
      resolve(window.JSZip);
    };
    script.onerror = () => reject(new Error("Failed to load JSZip"));
    document.head.appendChild(script);
  });
}
async function initializeLastSeenUpdates() {
  const chats = await getAllChatsFromIndexedDB();
  for (const chat of chats) {
    if (!chat.id) continue;
    lastSeenUpdates[chat.id] = {
      updatedAt: chat.updatedAt || Date.now(),
      hash: await generateHash(chat, "chat"),
    };
  }
}

function loadConfiguration() {
  if (!config) {
    config = {
      syncMode: "disabled",
      syncInterval: 15,
      bucketName: "",
      region: "",
      accessKey: "",
      secretKey: "",
      endpoint: "",
      encryptionKey: "",
    };
  }
  const urlParams = new URLSearchParams(window.location.search);
  const urlSyncMode = urlParams.get("syncMode");
  if (urlSyncMode && ["disabled", "backup", "sync"].includes(urlSyncMode)) {
    localStorage.setItem("sync-mode", urlSyncMode);
    logToConsole("info", `Sync mode set from URL parameter: ${urlSyncMode}`);
    urlParams.delete("syncMode");
    const newUrl =
      window.location.pathname +
      (urlParams.toString() ? `?${urlParams.toString()}` : "") +
      window.location.hash;
    window.history.replaceState({}, "", newUrl);
  }
  const storedConfig = {
    bucketName: localStorage.getItem("aws-bucket"),
    region: localStorage.getItem("aws-region"),
    accessKey: localStorage.getItem("aws-access-key"),
    secretKey: localStorage.getItem("aws-secret-key"),
    endpoint: localStorage.getItem("aws-endpoint"),
    syncInterval: parseInt(localStorage.getItem("backup-interval")) || 15,
    encryptionKey: localStorage.getItem("encryption-key"),
    syncMode: localStorage.getItem("sync-mode") || "disabled",
  };
  config = { ...config, ...storedConfig
  };
  config.syncMode = localStorage.getItem("sync-mode") || "disabled";
  return config;
}

function saveConfiguration() {
  localStorage.setItem("aws-bucket", config.bucketName);
  localStorage.setItem("aws-region", config.region);
  localStorage.setItem("aws-access-key", config.accessKey);
  localStorage.setItem("aws-secret-key", config.secretKey);
  localStorage.setItem("aws-endpoint", config.endpoint);
  localStorage.setItem("backup-interval", config.syncInterval.toString());
  localStorage.setItem("encryption-key", config.encryptionKey);
  localStorage.setItem("sync-mode", config.syncMode);
}
async function loadLocalMetadata() {
  let metadataInitialized = false;
  try {
    const storedMetadata = await getIndexedDBKey("sync-metadata");
    if (storedMetadata) {
      try {
        localMetadata = JSON.parse(storedMetadata);
        const formatLogTimestamp = (ts) =>
          ts ? new Date(ts).toLocaleString() : ts === 0 ? "0 (Epoch)" : ts;
        let settingsHashSamples = {};
        if (localMetadata.settings?.items) {
          const sampleKeys = Object.keys(localMetadata.settings.items).slice(
            0,
            3
          );
          if (sampleKeys.length > 0) {
            settingsHashSamples = sampleKeys.reduce((acc, key) => {
              acc[key] = localMetadata.settings.items[key]?.hash ?
                `${localMetadata.settings.items[key].hash.substring(0, 8)}...` :
                "none";
              return acc;
            }, {});
          }
        }
        logToConsole("debug", "Parsed localMetadata:", {
          lastSyncTime: formatLogTimestamp(localMetadata.lastSyncTime),
          hasChats: !!localMetadata.chats,
          chatCount: localMetadata.chats ?
            Object.keys(localMetadata.chats).length :
            0,
          firstChatSyncedAt: localMetadata.chats && Object.keys(localMetadata.chats).length > 0 ?
            formatLogTimestamp(
              localMetadata.chats[Object.keys(localMetadata.chats)[0]]
              ?.syncedAt
            ) :
            undefined,
          hasSettings: !!localMetadata.settings,
          settingsCount: localMetadata.settings?.items ?
            Object.keys(localMetadata.settings.items).length :
            0,
          settingsSyncedAt: formatLogTimestamp(
            localMetadata.settings?.syncedAt
          ),
          settingsSamples: settingsHashSamples,
        });
      } catch (parseError) {
        logToConsole(
          "error",
          "Failed to parse stored metadata, initializing from scratch",
          parseError
        );
        metadataInitialized = await initializeMetadataFromExistingData();
      }
    } else {
      logToConsole(
        "info",
        "No stored metadata found, initializing from existing data."
      );
      metadataInitialized = await initializeMetadataFromExistingData();
    }
  } catch (error) {
    logToConsole("error", "Failed to load local metadata:", error);
    try {
      logToConsole(
        "warning",
        "Attempting to recover by initializing fresh metadata."
      );
      metadataInitialized = await initializeMetadataFromExistingData();
      logToConsole(
        "success",
        "Successfully initialized fresh metadata after load error."
      );
    } catch (initError) {
      logToConsole(
        "error",
        "Failed to initialize fresh metadata after load error:",
        initError
      );
      throw error;
    }
  }
  return metadataInitialized;
}
async function initializeMetadataFromExistingData() {
  const chats = await getAllChatsFromIndexedDB();
  localMetadata = {
    chats: {},
    settings: {
      items: {},
      lastModified: Date.now(),
      syncedAt: 0,
    },
    lastSyncTime: 0,
  };
  for (const chat of chats) {
    if (!chat.id) continue;
    localMetadata.chats[chat.id] = {
      updatedAt: chat.updatedAt || Date.now(),
      hash: await generateHash(chat, "chat"),
      syncedAt: 0,
      isDeleted: false,
    };
  }
  logToConsole("success", "Metadata initialized from existing data");
  return true;
}
async function saveLocalMetadata() {
  try {
    const metadataToSave = JSON.stringify(localMetadata);
    logToConsole("debug", "Saving local metadata", {
      settingsCount: Object.keys(localMetadata.settings?.items || {}).length,
      chatsCount: Object.keys(localMetadata.chats || {}).length,
      lastModified: localMetadata.settings?.lastModified ?
        new Date(localMetadata.settings.lastModified).toISOString() :
        "unknown",
      syncedAt: localMetadata.settings?.syncedAt ?
        new Date(localMetadata.settings.syncedAt).toISOString() :
        "unknown",
      metadataSize: metadataToSave.length,
    });
    // if (localMetadata.settings?.items) {
    //   const sampleKeys = Object.keys(localMetadata.settings.items).slice(0, 3);
    //   if (sampleKeys.length > 0) {
    //     logToConsole(
    //       "debug",
    //       "Sample hashes being saved:",
    //       sampleKeys.reduce((acc, key) => {
    //         acc[key] = localMetadata.settings.items[key]?.hash
    //           ? `${localMetadata.settings.items[key].hash.substring(0, 8)}...`
    //           : "none";
    //         return acc;
    //       }, {})
    //     );
    //   }
    // }
    const formatLogTimestamp = (ts) =>
      ts ? new Date(ts).toLocaleString() : ts === 0 ? "0 (Epoch)" : ts;
    await setIndexedDBKey("sync-metadata", metadataToSave);
    const verifyMetadata = await getIndexedDBKey("sync-metadata");
    if (!verifyMetadata) {
      throw new Error(
        "Metadata save verification failed: No data returned from read verification"
      );
    }
    try {
      const parsedVerify = JSON.parse(verifyMetadata);
      const sampleKey = Object.keys(localMetadata.settings?.items || {})[0];

      if (
        sampleKey &&
        parsedVerify?.settings?.items?.[sampleKey]?.hash !==
        localMetadata.settings?.items?.[sampleKey]?.hash
      ) {
        // logToConsole("warning", "Metadata verification found hash mismatch", {
        //   key: sampleKey,
        //   expectedHash: localMetadata.settings?.items?.[sampleKey]?.hash
        //     ? `${localMetadata.settings.items[sampleKey].hash.substring(
        //         0,
        //         8
        //       )}...`
        //     : "none",
        //   savedHash: parsedVerify?.settings?.items?.[sampleKey]?.hash
        //     ? `${parsedVerify.settings.items[sampleKey].hash.substring(
        //         0,
        //         8
        //       )}...`
        //     : "none",
        // });
      } else {
        logToConsole(
          "success",
          "Local metadata saved and verified in IndexedDB"
        );
      }
    } catch (parseError) {
      logToConsole(
        "warning",
        "Error parsing verification metadata",
        parseError
      );
    }
  } catch (error) {
    logToConsole("error", "Failed to save local metadata:", error);
    throw error;
  }
}
async function generateHash(content, type = "generic") {
  let str;
  if (type === "chat" && content.id) {
    let messagesToProcess = content.messages || [];
    const stableChat = {
      folderID: content.folderID || null,
      messages: messagesToProcess
        .map((msg) => {
          if (!msg || typeof msg !== "object") return msg;
          const stableMsg = {};
          Object.keys(msg)
            .sort()
            .forEach((key) => {
              stableMsg[key] = msg[key];
            });
          return stableMsg;
        })
        .sort((a, b) => {
          if (a?.timestamp && b?.timestamp) {
            if (a.timestamp !== b.timestamp) return a.timestamp - b.timestamp;
          }
          if (a?.index !== undefined && b?.index !== undefined) {
            if (a.index !== b.index) return a.index - b.index;
          }
          const stringifyStable = (obj) =>
            JSON.stringify(obj, Object.keys(obj || {}).sort());
          return stringifyStable(a).localeCompare(stringifyStable(b));
        }),
      title: content.title || content.chatTitle || "",
    };
    str = JSON.stringify(stableChat, Object.keys(stableChat).sort());
  } else {
    str = typeof content === "string" ? content : JSON.stringify(content);
  }
  const msgBuffer = new TextEncoder().encode(str);
  const hashBuffer = await crypto.subtle.digest("SHA-256", msgBuffer);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  return hashArray.map((b) => b.toString(16).padStart(2, "0")).join("");
}
async function setupLocalStorageChangeListener() {
  window.addEventListener("storage", (e) => {
    if (!e.key || shouldExcludeSetting(e.key)) {
      return;
    }
    pendingSettingsChanges = true;
    logToConsole("info", `LocalStorage change detected: ${e.key}`);
    throttledCheckSyncStatus();
  });
  const originalSetItem = localStorage.setItem;
  localStorage.setItem = function(key, value) {
    const oldValue = localStorage.getItem(key);
    originalSetItem.apply(this, arguments);
    if (!shouldExcludeSetting(key) && oldValue !== value) {
      pendingSettingsChanges = true;
      logToConsole("info", `LocalStorage programmatic change detected: ${key}`);
      throttledCheckSyncStatus();
    }
  };
}
async function getPersistentDB() {
  if (persistentDB) {
    try {
      const transaction = persistentDB.transaction(["keyval"], "readonly");
      return persistentDB;
    } catch (error) {
      logToConsole(
        "warning",
        "Existing IndexedDB connection is stale, reconnecting"
      );
      await cleanupDBConnection();
    }
  }
  if (dbConnectionPromise) {
    try {
      return await dbConnectionPromise;
    } catch (error) {
      dbConnectionPromise = null;
    }
  }
  dbConnectionPromise = (async () => {
    try {
      persistentDB = await Promise.race([
        openIndexedDB(),
        new Promise((_, reject) =>
          setTimeout(
            () => reject(new Error("IndexedDB connection timeout")),
            DB_CONNECTION_TIMEOUT
          )
        ),
      ]);
      setupDBConnectionMonitoring();
      dbConnectionRetries = 0;
      return persistentDB;
    } catch (error) {
      dbConnectionPromise = null;
      dbConnectionRetries++;
      if (dbConnectionRetries < MAX_DB_RETRIES) {
        const delay = Math.min(
          DB_RETRY_DELAY * Math.pow(2, dbConnectionRetries - 1) +
          Math.random() * 1000,
          5000
        );
        logToConsole(
          "warning",
          `IndexedDB connection attempt ${dbConnectionRetries} failed, retrying in ${Math.round(
            delay / 1000
          )}s`,
          error
        );
        await new Promise((resolve) => setTimeout(resolve, delay));
        return getPersistentDB();
      }
      logToConsole("error", "Max IndexedDB connection retries reached", error);
      throw new Error(
        `Failed to establish IndexedDB connection after ${MAX_DB_RETRIES} attempts: ${error.message}`
      );
    }
  })();
  return dbConnectionPromise;
}

function setupDBConnectionMonitoring() {
  if (dbHeartbeatInterval) {
    clearInterval(dbHeartbeatInterval);
  }
  dbHeartbeatInterval = setInterval(async () => {
    if (!persistentDB) return;
    try {
      const transaction = persistentDB.transaction(["keyval"], "readonly");
      const store = transaction.objectStore("keyval");
      await new Promise((resolve, reject) => {
        const request = store.count();
        request.onsuccess = resolve;
        request.onerror = reject;
      });
    } catch (error) {
      logToConsole(
        "warning",
        "IndexedDB connection health check failed",
        error
      );
      await cleanupDBConnection();
    }
  }, 30000);
}
async function cleanupDBConnection() {
  try {
    if (dbHeartbeatInterval) {
      clearInterval(dbHeartbeatInterval);
      dbHeartbeatInterval = null;
    }
    if (persistentDB) {
      persistentDB.close();
      persistentDB = null;
    }
    dbConnectionPromise = null;
    logToConsole("info", "Cleaned up stale IndexedDB connection");
  } catch (error) {
    logToConsole("error", "Error cleaning up IndexedDB connection", error);
  }
}

function openIndexedDB() {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open("keyval-store", 1);
    request.onerror = () => {
      reject(
        new Error(
          `Failed to open IndexedDB: ${
            request.error?.message || "Unknown error"
          }`
        )
      );
    };
    request.onsuccess = (event) => {
      const db = event.target.result;
      db.onerror = (event) => {
        logToConsole("error", "IndexedDB error:", event.target.error);
        cleanupDBConnection();
      };
      db.onclose = () => {
        logToConsole("info", "IndexedDB connection closed");
        cleanupDBConnection();
      };
      db.onversionchange = () => {
        logToConsole("info", "IndexedDB version changed, closing connection");
        cleanupDBConnection();
      };
      resolve(db);
    };
    request.onupgradeneeded = (event) => {
      const db = event.target.result;
      if (!db.objectStoreNames.contains("keyval")) {
        db.createObjectStore("keyval");
      }
    };
    setTimeout(() => {
      if (!persistentDB) {
        reject(new Error("IndexedDB open request timed out"));
      }
    }, DB_CONNECTION_TIMEOUT);
  });
}
async function getAllChatsFromIndexedDB() {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open("keyval-store", 1);
    request.onerror = () => reject(request.error);
    request.onsuccess = (event) => {
      const db = event.target.result;
      const transaction = db.transaction(["keyval"], "readonly");
      const store = transaction.objectStore("keyval");
      const chats = [];
      store.getAllKeys().onsuccess = (keyEvent) => {
        const keys = keyEvent.target.result;
        const chatKeys = keys.filter((key) => key.startsWith("CHAT_"));
        if (chatKeys.length === 0) {
          resolve([]);
          return;
        }
        store.getAll().onsuccess = (valueEvent) => {
          const values = valueEvent.target.result;
          for (let i = 0; i < keys.length; i++) {
            const key = keys[i];
            if (key.startsWith("CHAT_")) {
              const chat = values[i];
              if (!chat.id) {
                chat.id = key.startsWith("CHAT_") ? key.slice(5) : key;
              }
              chats.push(chat);
            }
          }
          resolve(chats);
        };
      };
      transaction.oncomplete = () => {
        db.close();
      };
    };
    request.onupgradeneeded = (event) => {
      const db = event.target.result;
      if (!db.objectStoreNames.contains("keyval")) {
        db.createObjectStore("keyval");
      }
    };
  });
}
async function getChatFromIndexedDB(chatId) {
  return new Promise((resolve, reject) => {
    const key = chatId.startsWith("CHAT_") ? chatId : `CHAT_${chatId}`;
    const request = indexedDB.open("keyval-store", 1);
    request.onerror = () => reject(request.error);
    request.onsuccess = (event) => {
      const db = event.target.result;
      const transaction = db.transaction(["keyval"], "readonly");
      const store = transaction.objectStore("keyval");
      const getRequest = store.get(key);
      getRequest.onsuccess = () => {
        let fetchedChat = getRequest.result;
        logToConsole(
          "debug",
          `Chat fetched from IndexedDB (getChatFromIndexedDB): ${key}`, {
            hasChat: !!fetchedChat,
            hasMessages: !!fetchedChat?.messages,
            messagesLength: fetchedChat?.messages?.length,
          }
        );
        fetchedChat = standardizeChatMessages(fetchedChat);
        resolve(fetchedChat);
      };
      getRequest.onerror = () => {
        reject(getRequest.error);
      };
    };
  });
}
async function getIndexedDBKey(key) {
  const db = await openIndexedDB();
  return new Promise((resolve, reject) => {
    const transaction = db.transaction("keyval", "readonly");
    const store = transaction.objectStore("keyval");
    const request = store.get(key);
    request.onerror = () => reject(request.error);
    request.onsuccess = () => resolve(request.result);
  });
}
async function setIndexedDBKey(key, value) {
  const db = await openIndexedDB();
  return new Promise((resolve, reject) => {
    const transaction = db.transaction("keyval", "readwrite");
    const store = transaction.objectStore("keyval");
    const request = store.put(value, key);
    request.onerror = () => reject(request.error);
    request.onsuccess = () => resolve(request.result);
  });
}

function monitorIndexedDBForDeletions() {
  let knownChats = new Map();
  const MIN_CHAT_AGE_MS = 60 * 1000;
  const REQUIRED_MISSING_DETECTIONS = 2;
  let potentialDeletions = new Map();
  getAllChatsFromIndexedDB().then((chats) => {
    const now = Date.now();
    chats.forEach((chat) => {
      if (chat.id) {
        knownChats.set(chat.id, {
          detectedAt: now,
          confirmedCount: 3,
        });
      }
    });
    logToConsole(
      "info",
      `Initialized deletion monitor with ${knownChats.size} chats`
    );
  });
  setInterval(async () => {
    if (document.hidden) return;
    try {
      const now = Date.now();
      const currentChats = await getAllChatsFromIndexedDB();
      const currentChatIds = new Set(currentChats.map((chat) => chat.id));
      for (const chatId of currentChatIds) {
        if (knownChats.has(chatId)) {
          const chatInfo = knownChats.get(chatId);
          chatInfo.confirmedCount = Math.min(chatInfo.confirmedCount + 1, 5);
          if (potentialDeletions.has(chatId)) {
            potentialDeletions.delete(chatId);
          }
        } else {
          knownChats.set(chatId, {
            detectedAt: now,
            confirmedCount: 1,
          });
          /*
          updateChatMetadata(chatId, true)
            .then(() => {
              if (config.syncMode === "sync" || config.syncMode === "backup") {
                queueOperation(`new-chat-sync-${chatId}`, () =>
                  uploadChatToCloud(chatId)
                );
              }
            })
            .catch((error) => {
              logToConsole(
                "error",
                `Error updating metadata for new chat ${chatId}:`,
                error
              );
            });
          */
        }
      }
      for (const [chatId, chatInfo] of knownChats.entries()) {
        if (!currentChatIds.has(chatId)) {
          const isEstablishedChat =
            chatInfo.confirmedCount >= 2 &&
            now - chatInfo.detectedAt > MIN_CHAT_AGE_MS;
          if (isEstablishedChat) {
            const missingCount = (potentialDeletions.get(chatId) || 0) + 1;
            potentialDeletions.set(chatId, missingCount);
            if (missingCount >= REQUIRED_MISSING_DETECTIONS) {
              if (
                localMetadata.chats[chatId] &&
                localMetadata.chats[chatId].deleted === true
              ) {
                knownChats.delete(chatId);
                potentialDeletions.delete(chatId);
                continue;
              }
              logToConsole(
                "cleanup",
                `Confirmed deletion of chat ${chatId} (missing ${missingCount} times), creating tombstone`
              );
              localMetadata.chats[chatId] = {
                deleted: true,
                deletedAt: Date.now(),
                lastModified: Date.now(),
                syncedAt: 0,
                tombstoneVersion: 1,
                deletionSource: "indexeddb-monitor",
              };
              saveLocalMetadata();
              if (config.syncMode === "sync" || config.syncMode === "backup") {
                logToConsole(
                  "cleanup",
                  `Queueing deletion from cloud for chat ${chatId}`
                );
                queueOperation(`delete-chat-${chatId}`, () =>
                  deleteChatFromCloud(chatId)
                );
              }
              knownChats.delete(chatId);
              potentialDeletions.delete(chatId);
            } else {
              logToConsole(
                "info",
                `Chat ${chatId} appears to be missing (${missingCount}/${REQUIRED_MISSING_DETECTIONS} checks), waiting for confirmation`
              );
            }
          } else {
            if (potentialDeletions.has(chatId)) {
              const missingCount = potentialDeletions.get(chatId) + 1;
              if (missingCount > 5) {
                knownChats.delete(chatId);
                potentialDeletions.delete(chatId);
                logToConsole(
                  "info",
                  `Removed tracking for unstable new chat ${chatId}`
                );
              } else {
                potentialDeletions.set(chatId, missingCount);
              }
            } else {
              potentialDeletions.set(chatId, 1);
            }
          }
        }
      }
    } catch (error) {
      logToConsole("error", "Error in deletion monitor", error);
    }
  }, 10000);
}
async function saveChatToIndexedDB(chat, syncTimestamp = null) {
  return new Promise((resolve, reject) => {
    if (!chat || !chat.id) {
      reject(
        new Error("Cannot save chat: chat object or chat.id is undefined")
      );
      return;
    }
    const key = chat.id.startsWith("CHAT_") ? chat.id : `CHAT_${chat.id}`;
    const request = indexedDB.open("keyval-store", 1);
    request.onerror = () => reject(request.error);
    request.onsuccess = (event) => {
      const db = event.target.result;
      const transaction = db.transaction(["keyval"], "readwrite");
      const store = transaction.objectStore("keyval");
      if (chat.id.startsWith("CHAT_") && key !== chat.id) {
        chat.id = chat.id.slice(5);
      }
      chat.updatedAt = Date.now();
      const putRequest = store.put(chat, key);
      putRequest.onsuccess = () => {
        logToConsole("success", `Saved chat ${chat.id} to IndexedDB`);
        const isCloudOriginated = !!syncTimestamp;
        updateChatMetadata(
          chat.id,
          !isCloudOriginated,
          false,
          syncTimestamp,
          chat
        )
        .then(() => resolve())
        .catch(reject);
      };
      putRequest.onerror = () => reject(putRequest.error);
    };
    request.onupgradeneeded = (event) => {
      const db = event.target.result;
      if (!db.objectStoreNames.contains("keyval")) {
        db.createObjectStore("keyval");
      }
    };
  });
}
async function deleteChatFromIndexedDB(chatId) {
  return new Promise((resolve, reject) => {
    if (!chatId) {
      reject(new Error("Cannot delete chat: chatId is undefined"));
      return;
    }
    const key =
      typeof chatId === "string" && chatId.startsWith("CHAT_") ?
      chatId :
      `CHAT_${chatId}`;
    const request = indexedDB.open("keyval-store", 1);
    request.onerror = () => reject(request.error);
    request.onsuccess = (event) => {
      const db = event.target.result;
      const transaction = db.transaction(["keyval"], "readwrite");
      const store = transaction.objectStore("keyval");
      const deleteRequest = store.delete(key);
      deleteRequest.onsuccess = () => {
        logToConsole("success", `Deleted chat ${chatId} from IndexedDB`);
        resolve();
      };
      deleteRequest.onerror = () => reject(deleteRequest.error);
    };
  });
}

function initializeS3Client() {
  if (
    !config.accessKey ||
    !config.secretKey ||
    !config.region ||
    !config.bucketName
  ) {
    throw new Error("AWS configuration is incomplete");
  }
  const s3Config = {
    accessKeyId: config.accessKey,
    secretAccessKey: config.secretKey,
    region: config.region,
    // FIX: Aumentato timeout a 10 minuti per file grandi e connessioni lente
    httpOptions: {
      timeout: 600000,
      connectTimeout: 600000
    },
    maxRetries: 5
  };
  if (config.endpoint) {
    s3Config.endpoint = config.endpoint;
    s3Config.s3ForcePathStyle = true;
  }
  return new AWS.S3(s3Config);
}
async function listS3Objects(prefix = "") {
  const s3 = initializeS3Client();
  try {
    const params = {
      Bucket: config.bucketName,
      Prefix: prefix,
    };
    const response = await s3.listObjectsV2(params).promise();
    const objects = response.Contents || [];
    const objectsWithMetadata = await Promise.all(
      objects.map(async (obj) => {
        try {
          const headParams = {
            Bucket: config.bucketName,
            Key: obj.Key,
          };
          const headResponse = await s3.headObject(headParams).promise();
          return {
            ...obj,
            key: obj.Key,
            metadata: headResponse.Metadata || {},
          };
        } catch (error) {
          logToConsole(
            "error",
            `Failed to get metadata for ${obj.Key}:`,
            error
          );
          return {
            ...obj,
            key: obj.Key,
            metadata: {},
          };
        }
      })
    );
    return objectsWithMetadata;
  } catch (error) {
    logToConsole("error", "Failed to list S3 objects:", error);
    throw error;
  }
}
async function uploadToS3(key, data, metadata) {
  const s3 = initializeS3Client();
  try {
    let contentType = "application/octet-stream";
    if (key.endsWith(".json")) {
      contentType = "application/json";
    } else if (key.endsWith(".zip")) {
      contentType = "application/zip";
    }
    const params = {
      Bucket: config.bucketName,
      Key: key,
      Body: data,
      ContentType: contentType,
      ServerSideEncryption: "AES256",
      Metadata: metadata,
    };
    if (key === "metadata.json") {
      params.CacheControl = "no-cache, no-store, must-revalidate";
    }
    if (data.byteLength > 5 * 1024 * 1024) {
      await cleanupIncompleteMultipartUploads();
      const uploadId = await startMultipartUpload(key);
      const partSize = 5 * 1024 * 1024;
      const parts = [];
      for (let i = 0; i < data.byteLength; i += partSize) {
        const end = Math.min(i + partSize, data.byteLength);
        const chunk = data.slice(i, end);
        const partNumber = Math.floor(i / partSize) + 1;
        const part = await uploadPart(key, uploadId, partNumber, chunk);
        parts.push(part);
        const progress = Math.min(
          100,
          Math.round((end / data.byteLength) * 100)
        );
      }
      await completeMultipartUpload(key, uploadId, parts);
    } else {
      await s3.putObject(params).promise();
    }
    logToConsole("success", `Successfully uploaded to S3: ${key}`);
  } catch (error) {
    logToConsole("error", `Failed to upload to S3: ${key}`, error);
    throw error;
  }
}
async function downloadFromS3(key) {
  const s3 = initializeS3Client();
  try {
    const params = {
      Bucket: config.bucketName,
      Key: key,
    };
    const response = await s3.getObject(params).promise();
    const cleanMetadata = {};
    for (const [key, value] of Object.entries(response.Metadata || {})) {
      const cleanKey = key.replace("x-amz-meta-", "");
      cleanMetadata[cleanKey] = value;
    }
    if (key === "metadata.json") {
      return {
        data: response.Body,
        metadata: cleanMetadata,
      };
    }
    return {
      data: response.Body,
      metadata: cleanMetadata,
    };
  } catch (error) {
    if (error.code === "NoSuchKey") {
      logToConsole("info", `Object not found in S3: ${key}`);
      return null;
    }
    logToConsole("error", `Failed to download from S3: ${key}`, error);
    throw error;
  }
}
async function deleteFromS3(key) {
  const s3 = initializeS3Client();
  try {
    const params = {
      Bucket: config.bucketName,
      Key: key,
    };
    await s3.deleteObject(params).promise();
    logToConsole("success", `Successfully deleted from S3: ${key}`);
  } catch (error) {
    logToConsole("error", `Failed to delete from S3: ${key}`, error);
    throw error;
  }
}
async function startMultipartUpload(key) {
  const s3 = initializeS3Client();
  try {
    let contentType = "application/octet-stream";
    if (key.endsWith(".json")) {
      contentType = "application/json";
    } else if (key.endsWith(".zip")) {
      contentType = "application/zip";
    }
    const params = {
      Bucket: config.bucketName,
      Key: key,
      ContentType: contentType,
      ServerSideEncryption: "AES256",
    };
    const response = await s3.createMultipartUpload(params).promise();
    return response.UploadId;
  } catch (error) {
    logToConsole("error", "Failed to start multipart upload:", error);
    throw error;
  }
}
async function uploadPart(key, uploadId, partNumber, data) {
  const s3 = initializeS3Client();
  const maxRetries = 3;
  const baseDelay = 1000;
  let retryCount = 0;
  let lastError = null;
  let uploadSuccess = false;
  while (!uploadSuccess && retryCount <= maxRetries) {
    try {
      const params = {
        Bucket: config.bucketName,
        Key: key,
        UploadId: uploadId,
        PartNumber: partNumber,
        Body: data,
      };
      if (retryCount > 0) {
        logToConsole(
          "info",
          `Retrying upload part ${partNumber} (attempt ${retryCount + 1}/${
            maxRetries + 1
          })`
        );
      }
      const response = await s3.uploadPart(params).promise();
      uploadSuccess = true;
      return {
        ETag: response.ETag,
        PartNumber: partNumber,
      };
    } catch (error) {
      lastError = error;
      retryCount++;
      logToConsole(
        "error",
        `Error uploading part ${partNumber} (attempt ${retryCount}/${
          maxRetries + 1
        }):`,
        error
      );
      if (retryCount > maxRetries) {
        logToConsole(
          "error",
          `All retries failed for part ${partNumber}, aborting multipart upload`
        );
        try {
          await abortMultipartUpload(key, uploadId);
        } catch (abortError) {
          logToConsole("error", "Error aborting multipart upload:", abortError);
        }
        throw new Error(
          `Failed to upload part ${partNumber} after ${
            maxRetries + 1
          } attempts: ${lastError.message}`
        );
      }
      const delay = Math.min(
        baseDelay * Math.pow(2, retryCount - 1) + Math.random() * 1000,
        30000
      );
      logToConsole(
        "info",
        `Retrying part ${partNumber} in ${Math.round(delay / 1000)} seconds`
      );
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }
}
async function completeMultipartUpload(key, uploadId, parts) {
  const s3 = initializeS3Client();
  try {
    const params = {
      Bucket: config.bucketName,
      Key: key,
      UploadId: uploadId,
      MultipartUpload: {
        Parts: parts,
      },
    };
    await s3.completeMultipartUpload(params).promise();
    logToConsole("success", "Multipart upload completed successfully");
  } catch (error) {
    logToConsole("error", "Failed to complete multipart upload:", error);
    throw error;
  }
}
async function abortMultipartUpload(key, uploadId) {
  const s3 = initializeS3Client();
  try {
    const params = {
      Bucket: config.bucketName,
      Key: key,
      UploadId: uploadId,
    };
    await s3.abortMultipartUpload(params).promise();
    logToConsole("warning", "Multipart upload aborted");
  } catch (error) {
    logToConsole("error", "Failed to abort multipart upload:", error);
    throw error;
  }
}
async function cleanupIncompleteMultipartUploads() {
  const s3 = initializeS3Client();
  try {
    const multipartUploads = await s3
      .listMultipartUploads({
        Bucket: config.bucketName,
      })
      .promise();
    if (multipartUploads.Uploads && multipartUploads.Uploads.length > 0) {
      logToConsole(
        "cleanup",
        `Found ${multipartUploads.Uploads.length} incomplete multipart uploads`
      );
      for (const upload of multipartUploads.Uploads) {
        const uploadAge = Date.now() - new Date(upload.Initiated).getTime();
        const fiveMinutes = 5 * 60 * 1000;
        if (uploadAge > fiveMinutes) {
          try {
            await s3
              .abortMultipartUpload({
                Bucket: config.bucketName,
                Key: upload.Key,
                UploadId: upload.UploadId,
              })
              .promise();
            logToConsole(
              "success",
              `Aborted incomplete upload for ${upload.Key} (${Math.round(
                uploadAge / 1000 / 60
              )}min old)`
            );
          } catch (error) {
            logToConsole(
              "error",
              `Failed to abort upload for ${upload.Key}:`,
              error
            );
          }
        } else {
          logToConsole(
            "skip",
            `Skipping recent upload for ${upload.Key} (${Math.round(
              uploadAge / 1000
            )}s old)`
          );
        }
      }
    } else {

    }
  } catch (error) {
    logToConsole("error", "Error cleaning up multipart uploads:", error);
  }
}
async function deriveKey(password) {
  const encoder = new TextEncoder();
  const keyMaterial = await window.crypto.subtle.importKey(
    "raw",
    encoder.encode(password), {
      name: "PBKDF2"
    },
    false, ["deriveBits", "deriveKey"]
  );
  const key = await window.crypto.subtle.deriveKey({
      name: "PBKDF2",
      salt: encoder.encode("typingmind-backup-salt"),
      iterations: 100000,
      hash: "SHA-256",
    },
    keyMaterial, {
      name: "AES-GCM",
      length: 256
    },
    true, ["encrypt", "decrypt"]
  );
  return key;
}
async function safeStringify(data) {
  try {
    if (typeof data === "string") {
      return data;
    }
    const chunkSize = 50000;
    let result = "{";
    const keys = Object.keys(data);
    for (let i = 0; i < keys.length; i++) {
      const key = keys[i];
      let value = data[key];
      if (value === undefined) {
        value = null;
      }
      if (i > 0) result += ",";
      result += `"${key}":`;
      if (typeof value === "object" && value !== null) {
        if (Array.isArray(value)) {
          result += "[";
          for (let j = 0; j < value.length; j++) {
            if (j > 0) result += ",";
            let arrayValue = value[j];
            if (arrayValue === undefined) {
              arrayValue = null;
            }
            // FIX: Gestione sicura per array
            const stringifiedArr = JSON.stringify(arrayValue);
            result += (stringifiedArr === undefined ? "null" : stringifiedArr);

            if (result.length > chunkSize) {
              await new Promise((resolve) => setTimeout(resolve, 0));
            }
          }
          result += "]";
        } else {
          const objKeys = Object.keys(value);
          result += "{";
          for (let j = 0; j < objKeys.length; j++) {
            const objKey = objKeys[j];
            if (j > 0) result += ",";
            let objValue = value[objKey];
            if (objValue === undefined) {
              objValue = null;
            }
            // FIX: Gestione sicura per oggetti
            const stringifiedObj = JSON.stringify(objValue);
            result += `"${objKey}":${stringifiedObj === undefined ? "null" : stringifiedObj}`;

            if (result.length > chunkSize) {
              await new Promise((resolve) => setTimeout(resolve, 0));
            }
          }
          result += "}";
        }
      } else {
        // FIX: Gestione sicura per valori primitivi
        const stringifiedVal = JSON.stringify(value);
        result += (stringifiedVal === undefined ? "null" : stringifiedVal);
      }
      if (result.length > chunkSize) {
        await new Promise((resolve) => setTimeout(resolve, 0));
      }
    }
    result += "}";
    return result;
  } catch (error) {
    logToConsole(
      "warning",
      "Safe stringify failed, falling back to regular JSON.stringify",
      error
    );
    return JSON.stringify(data, (key, value) =>
      value === undefined ? null : value
    );
  }
}
async function encryptData(data) {
  const encryptionKey = localStorage.getItem("encryption-key");
  if (!encryptionKey) {
    logToConsole("warning", "No encryption key found");
    throw new Error("Encryption key not configured");
  }
  try {
    const key = await deriveKey(encryptionKey);
    const enc = new TextEncoder();
    const iv = window.crypto.getRandomValues(new Uint8Array(12));
    const jsonString = await safeStringify(data);
    const encodedData = enc.encode(jsonString);
    const encryptedContent = await window.crypto.subtle.encrypt({
        name: "AES-GCM",
        iv: iv,
      },
      key,
      encodedData
    );
    const marker = new TextEncoder().encode("ENCRYPTED:");
    const combinedData = new Uint8Array(
      marker.length + iv.length + encryptedContent.byteLength
    );
    combinedData.set(marker);
    combinedData.set(iv, marker.length);
    combinedData.set(
      new Uint8Array(encryptedContent),
      marker.length + iv.length
    );
    return combinedData;
  } catch (error) {
    logToConsole("error", "Encryption failed:", error);
    throw error;
  }
}
async function decryptData(data) {
  const marker = "ENCRYPTED:";
  const dataString = new TextDecoder().decode(data.slice(0, marker.length));
  const bucketName = localStorage.getItem("aws-bucket");
  logToConsole("tag", "Checking encryption marker:", {
    expectedMarker: marker,
    foundMarker: dataString,
    isEncrypted: dataString === marker,
  });
  if (dataString !== marker) {
    logToConsole("info", "Data is not encrypted, returning as-is");
    return JSON.parse(new TextDecoder().decode(data));
  }
  if (!bucketName) {
    logToConsole("info", "Backup not configured, skipping decryption");
    throw new Error("Backup not configured");
  }
  const encryptionKey = localStorage.getItem("encryption-key");
  if (!encryptionKey) {
    logToConsole("error", "Encrypted data found but no key provided");
    if (backupIntervalRunning) {
      clearInterval(backupInterval);
      backupIntervalRunning = false;
    }
    wasImportSuccessful = false;
    await showCustomAlert(
      "Please configure your encryption key in the backup settings before proceeding.",
      "Configuration Required"
    );
    throw new Error("Encryption key not configured");
  }
  try {
    const key = await deriveKey(encryptionKey);
    const iv = data.slice(marker.length, marker.length + 12);
    const encryptedData = data.slice(marker.length + 12);
    const decryptedContent = await window.crypto.subtle.decrypt({
        name: "AES-GCM",
        iv: iv,
      },
      key,
      encryptedData
    );
    const decryptedText = new TextDecoder().decode(decryptedContent);
    logToConsole("success", "Decryption successful");
    return decryptedText;
  } catch (error) {
    logToConsole("error", "Decryption failed:", error);
    throw new Error(
      "Failed to decrypt backup. Please check your encryption key."
    );
  }
}

function startBackupIntervals() {
  startSyncInterval();
}
async function checkAndPerformDailyBackup() {
  try {
    const lastBackupStr = localStorage.getItem("last-daily-backup");
    const now = new Date();
    const currentDateStr = `${now.getFullYear()}${String(
      now.getMonth() + 1
    ).padStart(2, "0")}${String(now.getDate()).padStart(2, "0")}`;
    if (!lastBackupStr || lastBackupStr !== currentDateStr) {
      logToConsole("info", "Starting daily backup...");
      await performDailyBackup();
      localStorage.setItem("last-daily-backup", currentDateStr);
      logToConsole("success", "Daily backup completed");
    } else {
      logToConsole("skip", "Daily backup already performed today");
    }
  } catch (error) {
    logToConsole("error", "Error checking/performing daily backup:", error);
  }
}
async function performDailyBackup() {
  backupState.isBackupInProgress = true;
  try {
    await loadJSZip();
    const today = new Date();
    const dateString = `${today.getFullYear()}${String(
      today.getMonth() + 1
    ).padStart(2, "0")}${String(today.getDate()).padStart(2, "0")}`;
    const key = `typingmind-backup-${dateString}.zip`;
    const data = await exportBackupData();
    const success = await createDailyBackup(key, data);
    if (success) {
      await cleanupOldBackups("daily");
      backupState.lastDailyBackup = Date.now();
      logToConsole("success", "Daily backup created successfully");
      return true;
    } else {
      logToConsole("error", "Daily backup creation failed");
      return false;
    }
  } catch (error) {
    logToConsole("error", "Daily backup failed:", error);
    return false;
  } finally {
    backupState.isBackupInProgress = false;
  }
}

function exportBackupData() {
  return new Promise((resolve, reject) => {
    // FIX: Estrazione pulita di localStorage per evitare funzioni di sistema
    const cleanLocalStorage = {};
    try {
      Object.keys(localStorage).forEach(key => {
        const val = localStorage.getItem(key);
        if (val !== null) {
          cleanLocalStorage[key] = val;
        }
      });
    } catch (e) {
      console.error("Error accessing localStorage", e);
    }

    const exportData = {
      localStorage: cleanLocalStorage,
      indexedDB: {},
    };

    logToConsole("info", "Starting data export", {
      localStorageKeys: Object.keys(exportData.localStorage).length,
    });
    const request = indexedDB.open("keyval-store", 1);
    request.onerror = () => reject(request.error);
    request.onsuccess = function(event) {
      const db = event.target.result;
      const transaction = db.transaction(["keyval"], "readonly");
      const store = transaction.objectStore("keyval");

      // FIX: Gestione asincrona per convertire i BLOB (immagini) in Base64
      const collectData = new Promise((resolveData) => {
        store.getAllKeys().onsuccess = function(keyEvent) {
          const keys = keyEvent.target.result;
          store.getAll().onsuccess = async function(valueEvent) {
            const values = valueEvent.target.result;

            // Processiamo i valori uno per uno per cercare i Blob
            for (let i = 0; i < keys.length; i++) {
              let val = values[i];
              const key = keys[i];

              // Se è un Blob (File/Immagine), convertiamolo
              if (val instanceof Blob) {
                try {
                  const b64 = await blobToDataURL(val);
                  exportData.indexedDB[key] = {
                    __is_blob: true,
                    data: b64
                  };
                } catch (err) {
                  logToConsole("error", `Failed to convert blob for key ${key}`, err);
                }
              } else {
                exportData.indexedDB[key] = val;
              }
            }
            resolveData();
          };
        };
      });

      Promise.all([
          collectData,
          new Promise((resolveTransaction) => {
            transaction.oncomplete = resolveTransaction;
          }),
        ])
        .then(() => {
          resolve(exportData);
        })
        .catch(reject);
      transaction.onerror = () => reject(transaction.error);
    };
  });
}
async function createDailyBackup(key, data) {
  logToConsole("start", `Creating daily backup: ${key}`);
  try {
    const JSZip = await loadJSZip();
    if (!JSZip) {
      throw new Error("Failed to load JSZip library");
    }
    const encryptionKey = localStorage.getItem("encryption-key");
    if (!encryptionKey) {
      logToConsole(
        "warning",
        "No encryption key found, backup will not be encrypted"
      );
      return false;
    }
    logToConsole("info", "Encrypting backup data...");
    const encryptedData = await encryptData(data);
    const rawSize = Math.round(encryptedData.length * 0.8);
    logToConsole("info", `Estimated raw data size: ${formatFileSize(rawSize)}`);
    const zip = new JSZip();
    const jsonFileName = key.replace(".zip", ".json");
    zip.file(jsonFileName, encryptedData, {
      compression: "DEFLATE",
      compressionOptions: {
        level: 9,
      },
      binary: true,
    });
    const compressedContent = await zip.generateAsync({
      type: "blob"
    });
    if (compressedContent.size < 100) {
      throw new Error(
        "Daily backup file is too small or empty. Upload cancelled."
      );
    }
    const arrayBuffer = await compressedContent.arrayBuffer();
    const content = new Uint8Array(arrayBuffer);
    const uploadMetadata = {
      version: EXTENSION_VERSION,
      timestamp: String(Date.now()),
      type: "daily",
      originalSize: String(rawSize),
      compressedSize: String(compressedContent.size),
      encrypted: "true",
    };
    await uploadToS3(key, content, uploadMetadata);
    logToConsole("success", "Daily backup created successfully");
    return true;
  } catch (error) {
    logToConsole("error", "Daily backup creation failed:", error);
    return false;
  }
}
async function createSnapshot(name) {
  logToConsole("start", "Creating snapshot...");
  backupState.isBackupInProgress = true;
  try {
    logToConsole("info", "Loading JSZip...");
    await loadJSZip();
    logToConsole("success", "JSZip loaded successfully");
    const data = await exportBackupData();
    const now = new Date();
    const timestamp =
      now.getFullYear().toString() +
      (now.getMonth() + 1).toString().padStart(2, "0") +
      now.getDate().toString().padStart(2, "0") +
      now.getHours().toString().padStart(2, "0") +
      now.getMinutes().toString().padStart(2, "0") +
      now.getSeconds().toString().padStart(2, "0") +
      now.getMilliseconds().toString().padStart(3, "0");
    const key = `s-${name}-${timestamp}.zip`;
    logToConsole("info", `Creating snapshot with key: ${key}`);
    const encryptionKey = localStorage.getItem("encryption-key");
    if (!encryptionKey) {
      logToConsole(
        "warning",
        "No encryption key found, snapshot will not be encrypted"
      );
      return false;
    }
    logToConsole("info", "Encrypting snapshot data...");
    const encryptedData = await encryptData(data);
    const rawSize = Math.round(encryptedData.length * 0.8);
    logToConsole("info", `Estimated raw data size: ${formatFileSize(rawSize)}`);
    const zip = new JSZip();
    const jsonFileName = key.replace(".zip", ".json");
    zip.file(jsonFileName, encryptedData, {
      compression: "DEFLATE",
      compressionOptions: {
        level: 9,
      },
      binary: true,
    });
    const compressedContent = await zip.generateAsync({
      type: "blob"
    });
    if (compressedContent.size < 100) {
      throw new Error("Snapshot file is too small or empty. Upload cancelled.");
    }
    const arrayBuffer = await compressedContent.arrayBuffer();
    const content = new Uint8Array(arrayBuffer);
    const uploadMetadata = {
      version: EXTENSION_VERSION,
      timestamp: String(Date.now()),
      type: "snapshot",
      originalSize: String(rawSize),
      compressedSize: String(compressedContent.size),
      encrypted: "true",
    };
    await uploadToS3(key, content, uploadMetadata);
    backupState.lastManualSnapshot = Date.now();
    await loadBackupList();
    logToConsole("success", "Snapshot created successfully");
    return true;
  } catch (error) {
    logToConsole("error", "Snapshot creation failed:", error);
    return false;
  } finally {
    backupState.isBackupInProgress = false;
  }
}
async function cleanupOldBackups(type) {
  logToConsole("start", `Cleaning up old ${type} backups...`);
  try {
    let prefix;
    let keepCount;
    switch (type) {
      case "daily":
        prefix = config.dailyBackupPrefix;
        keepCount = config.keepDailyBackups;
        break;
      default:
        return;
    }
    const objects = await listS3Objects(prefix);
    objects.sort((a, b) => b.LastModified - a.LastModified);
    for (let i = keepCount; i < objects.length; i++) {
      await deleteFromS3(objects[i].Key);
      logToConsole("cleanup", `Deleted old backup: ${objects[i].Key}`);
    }
    logToConsole("success", `Cleanup of old ${type} backups completed`);
  } catch (error) {
    logToConsole("error", `Failed to clean up old ${type} backups:`, error);
  }
}
async function restoreFromBackup(key) {
  logToConsole("start", `Starting restore from backup: ${key}`);
  try {
    operationState.isImporting = true;
    const backup = await downloadFromS3(key);
    if (!backup || !backup.data) {
      throw new Error("Backup not found or empty");
    }
    let backupContent;
    if (key.endsWith(".zip")) {
      const JSZip = await loadJSZip();
      const zip = await JSZip.loadAsync(backup.data);
      const jsonFile = Object.keys(zip.files).find((f) => f.endsWith(".json"));
      if (!jsonFile) {
        throw new Error("No JSON file found in backup");
      }
      backupContent = await zip.file(jsonFile).async("uint8array");
    } else {
      backupContent = backup.data;
    }
    logToConsole("info", "Decrypting backup content...");
    const decryptedContent = await decryptData(backupContent);
    logToConsole("info", "Decrypted content type:", typeof decryptedContent);

    // === FIX PER BACKUP VECCHI/CORROTTI ===
    // 1. Rimuoviamo specificamente le funzioni di sistema salvate per errore (setItem, etc.)
    // Questo elimina la causa principale dell'errore "Unexpected token u"
    let sanitizedContent = decryptedContent
      .replace(/"setItem"\s*:\s*undefined\s*,?/g, "")
      .replace(/"getItem"\s*:\s*undefined\s*,?/g, "")
      .replace(/"removeItem"\s*:\s*undefined\s*,?/g, "")
      .replace(/"clear"\s*:\s*undefined\s*,?/g, "")
      .replace(/"key"\s*:\s*undefined\s*,?/g, "")
      .replace(/"length"\s*:\s*undefined\s*,?/g, "");

    // 2. Sostituzione generica di altri undefined rimasti con null
    sanitizedContent = sanitizedContent
      .replace(/:\s*undefined\b/g, ": null")
      .replace(/,\s*undefined\b/g, ", null")
      .replace(/\[\s*undefined\b/g, "[null");
    // ======================================

    let parsedContent;
    try {
      logToConsole("info", "Attempting to parse sanitized content...");
      parsedContent = JSON.parse(sanitizedContent);

      logToConsole("info", "Parsed content structure:", {
        type: typeof parsedContent,
        hasLocalStorage: !!parsedContent.localStorage,
        hasIndexedDB: !!parsedContent.indexedDB,
      });
    } catch (error) {
      logToConsole("error", "JSON parse error:", error);
      // Mostra un'anteprima dell'errore per debug
      const errorPos = sanitizedContent.indexOf("undefined");
      if (errorPos > -1) {
        logToConsole("error", "Found residual 'undefined' at pos: " + errorPos, sanitizedContent.substring(errorPos - 20, errorPos + 20));
      }
      throw new Error(`Failed to parse backup data: ${error.message}`);
    }

    if (!parsedContent || typeof parsedContent !== "object") {
      throw new Error("Invalid backup format: Root content is not an object");
    }

    logToConsole("info", "Importing data to storage...");
    // Qui chiamerà la tua importDataToStorage (che hai aggiornato prima per gli allegati)
    await importDataToStorage(parsedContent);

    const chats = await getAllChatsFromIndexedDB();
    const currentTime = new Date().toLocaleString();
    localStorage.setItem("last-cloud-sync", currentTime);
    await saveLocalMetadata();
    operationState.isImporting = false;
    logToConsole("success", "Backup restored successfully");
    return true;
  } catch (error) {
    logToConsole("error", "Restore failed:", error);
    operationState.isImporting = false;
    throw error;
  }
}

function importDataToStorage(data) {
  return new Promise((resolve, reject) => {
    const preserveKeys = [
      "encryption-key",
      "aws-bucket",
      "aws-access-key",
      "aws-secret-key",
      "aws-region",
      "aws-endpoint",
      "backup-interval",
      "sync-mode",
      "sync-status-hidden",
      "sync-status-position",
      "last-daily-backup",
      "last-cloud-sync",
      "chat-sync-metadata",
    ];
    let settingsRestored = 0;
    let filesRestored = 0;

    // Funzione helper per rilevare se un oggetto è un file serializzato
    const tryReviveBlob = (value, key) => {
      // Se è già un blob o nullo, ritorna
      if (!value) return value;
      if (value instanceof Blob) return value;

      // 1. Nuovo formato (Backup futuri)
      if (typeof value === 'object' && value.__is_blob === true && value.data) {
        return dataURLToBlob(value.data);
      }

      // 2. Vecchio formato: Buffer Node.js o Array tipizzati
      if (typeof value === 'object' && value.type === 'Buffer' && Array.isArray(value.data)) {
        return new Blob([new Uint8Array(value.data)]);
      }

      // 3. Euristica per i backup da 300MB (Array di numeri grezzi)
      // Se è un array e contiene numeri, ed è abbastanza lungo, è probabilmente un file
      if (Array.isArray(value) && value.length > 0 && typeof value[0] === 'number') {
        // Controllo euristico: se la chiave suggerisce un file o l'array è grande
        if (value.length > 100 || (typeof key === 'string' && (key.startsWith('FILE_') || key.includes('-')))) {
          return new Blob([new Uint8Array(value)]);
        }
      }

      // 4. Euristica per oggetti "Array-like" {0: x, 1: y, length: z}
      if (typeof value === 'object' && value !== null && value[0] !== undefined && (value.length !== undefined || Object.keys(value).length > 50)) {
        // Tentativo di conversione in array
        try {
          const arr = Object.values(value).filter(v => typeof v === 'number');
          if (arr.length > 0) {
            return new Blob([new Uint8Array(arr)]);
          }
        } catch (e) {
          /* ignore */ }
      }

      // 5. FIX PER OGGETTI "BASE64" (Backup recenti/AI Images)
      // Se troviamo un oggetto { base64: "data:..." }, lo convertiamo in Blob
      if (typeof value === 'object' && value !== null && typeof value.base64 === 'string' && value.base64.startsWith('data:')) {
        // Convertiamo solo se la chiave suggerisce che è un file, oppure se è in una cache
        if (typeof key === 'string' && (key.startsWith('FILE_') || key.startsWith('CLIENT_CACHE_'))) {
          return dataURLToBlob(value.base64);
        }
      }

      return value;
    };

    // Ripristino LocalStorage
    if (data.localStorage) {
      Object.entries(data.localStorage).forEach(([key, settingData]) => {
        if (!preserveKeys.includes(key)) {
          try {
            const value = (typeof settingData === "object" && settingData !== null && settingData.data !== undefined) ?
              settingData.data :
              settingData;

            const source = (typeof settingData === "object" && settingData !== null && settingData.source) ?
              settingData.source :
              "localStorage";

            if (source === "indexeddb") return;

            localStorage.setItem(key, value);
            settingsRestored++;
          } catch (error) {
            logToConsole("error", `Error restoring localStorage setting ${key}:`, error);
          }
        }
      });
    }

    // Ripristino IndexedDB
    if (data.indexedDB) {
      const request = indexedDB.open("keyval-store");
      request.onerror = () => reject(request.error);
      request.onsuccess = function(event) {
        const db = event.target.result;
        const transaction = db.transaction(["keyval"], "readwrite");
        const objectStore = transaction.objectStore("keyval");

        transaction.oncomplete = () => {
          logToConsole("success", `Restoration completed`, {
            settings: settingsRestored,
            filesAttempted: filesRestored,
            timestamp: new Date().toISOString(),
          });
          resolve();
        };

        transaction.onerror = () => reject(transaction.error);

        const deleteRequest = objectStore.clear();
        deleteRequest.onsuccess = function() {
          Object.entries(data.indexedDB).forEach(([key, value]) => {
            if (!preserveKeys.includes(key)) {
              try {
                let valueToStore = value;

                // Tentativo aggressivo di conversione in stringa JSON prima di analizzare
                // Molti vecchi backup salvavano stringhe JSON dentro IndexedDB
                if (typeof valueToStore === "string" && (valueToStore.startsWith("{") || valueToStore.startsWith("["))) {
                  try {
                    const parsed = JSON.parse(valueToStore);
                    if (typeof parsed === 'object') {
                      valueToStore = parsed;
                    }
                  } catch (e) {
                    /* Era solo una stringa */
                  }
                }

                // Tentativo di "Reviving" del file binario (Blob/Base64/ArrayBuffer)
                const revived = tryReviveBlob(valueToStore, key);

                if (revived instanceof Blob) {
                  valueToStore = revived;
                  filesRestored++;
                  // Log per debuggare se stiamo effettivamente recuperando file
                  if (filesRestored <= 5) {
                    logToConsole("info", `Restored potential file for key: ${key}`, {
                      size: valueToStore.size
                    });
                  }
                }

                if (valueToStore !== null && valueToStore !== undefined) {
                  objectStore.put(valueToStore, key);
                  // Se non era un file, contiamo come setting generico
                  if (!(valueToStore instanceof Blob)) settingsRestored++;
                }
              } catch (error) {
                logToConsole("error", `Error restoring IndexedDB key ${key}:`, error);
              }
            }
          });
        };
      };
    } else {
      logToConsole("success", `Settings restore completed (LocalStorage only)`, {
        totalRestored: settingsRestored
      });
      resolve();
    }
  });
}

// --- HELPER FUNCTIONS PER GLI ALLEGATI ---

function blobToDataURL(blob) {
  return new Promise((resolve) => {
    const reader = new FileReader();
    reader.onloadend = () => resolve(reader.result);
    reader.readAsDataURL(blob);
  });
}

function dataURLToBlob(dataURL) {
  try {
    const arr = dataURL.split(',');
    const mimeMatch = arr[0].match(/:(.*?);/);
    const mime = mimeMatch ? mimeMatch[1] : 'application/octet-stream';
    const bstr = atob(arr[1]);
    let n = bstr.length;
    const u8arr = new Uint8Array(n);
    while (n--) {
      u8arr[n] = bstr.charCodeAt(n);
    }
    return new Blob([u8arr], {
      type: mime
    });
  } catch (e) {
    console.error("Error converting DataURL to Blob", e);
    return null;
  }
}

window.addEventListener("unload", resetOperationStates);
window.addEventListener("visibilitychange", () => {
  if (document.hidden) {
    resetOperationStates();
  }
});