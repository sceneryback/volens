"use strict";

const $ = (selector) => document.querySelector(selector);

const state = {
  analyzing: false,
  branchesReady: false,
  podsReady: false,
  language: localStorage.getItem("volens-language") || "en",
  progressTimer: null,
  progressStage: 0,
  report: null,
};

const I18N = {
  en: {
    brandKicker: "VOLCANO SCHEDULER DIAGNOSTICS",
    brandSubtitle: "Diagnose Pending pods in scheduler order: enqueue → JobValid → allocate",
    reload: "Refresh targets",
    analyze: "Analyze",
    podLabel: "Pending Pod",
    branchLabel: "Volcano branch",
    loading: "Loading",
    loadingPods: "Loading Pending pods…",
    podsLoadFailed: "Failed to load Pending pods",
    noPods: "No Pending pods",
    noPodsStatus: "No Pending pod is available for analysis",
    podsFound: (count) => `${count} Pending pod${count === 1 ? "" : "s"} found`,
    loadingBranches: "Loading remote branches…",
    branchesLoadFailed: "Failed to load Volcano branches",
    noBranches: "No remote branch is available",
    noBranchesStatus: "No branch is available in the repository",
    branchesFound: (count, note) => `${count} remote branch${count === 1 ? "" : "es"} loaded${note}`,
    schedulerDefaultBranch: (version, branch) => `, scheduler ${version} recommends ${branch}`,
    schedulerVersionFailed: (message) => `, version detection failed: ${message}`,
    invalidJSON: (status) => `Invalid service response (HTTP ${status})`,
    selectTarget: "Select a Pending Pod and Volcano branch first",
    analyzeFailed: "Analysis failed",
    analyzeStatus: (branch) => `Collecting Volcano scheduling evidence from ${branch}…`,
    progressStages: [
      "Preflight and workload checks",
      "Queue, PodGroup, and enqueue rules",
      "Volcano cache and node filters",
      "Source/log fallback and LLM analysis if rules are inconclusive",
    ],
    progressHint: "The report will refresh when the current analysis finishes.",
    waiting: "Waiting for analysis result",
    summarySmall: "SUMMARY",
    summaryTitle: "Summary",
    rootCause: "Root cause",
    suggestions: "Suggestions",
    noConclusion: "No conclusion yet",
    enqueueStage: "Enqueue checks",
    queueRuntime: "Queue runtime resources and JobEnqueueable",
    runtimeUnavailable: "Runtime snapshot unavailable",
    unknownStrategy: "Unknown",
    queueHeader: "Queue",
    strategyHeader: "Strategy",
    cap: "realCapability",
    alloc: "Allocated",
    inq: "Inqueue",
    req: "Queue request",
    cand: "Current PodGroup MinResources",
    elastic: "Elastic resources",
    avail: "Cap−Alloc−InQ+Elastic",
    need: "Cand+Alloc+InQ−Elastic",
    queueFormula: "Enqueue formula",
    proportionFormulaNote: "proportion uses this check for enqueue; allocate later uses fair-share deserved − allocated.",
    capacityFormulaNote: "capacity uses the same enqueue check; allocate later uses realCapability − allocated and Queue.spec.deserved for share/order.",
    noQueueFormulaNote: "No enabled proportion/capacity JobEnqueueable rule was found.",
    podGroupsTitle: "PodGroups in queue",
    items: (count) => `${count} item${count === 1 ? "" : "s"}`,
    podGroupHeader: "PodGroup / Namespace",
    priority: "Priority",
    phase: "Phase",
    noPodGroups: "No PodGroup is available from informer evidence",
    resourceLegend: "PodGroup spec.minResources / requests.*",
    resourceColumn: "Resource columns",
    priorityLegend: "PriorityClass numeric value / name",
    minLegend: "minMember",
    jobValidStage: "Job validity (JobValid inside allocate)",
    jobValidNote: "JobValid receives the whole Job/PodGroup. Container names are only shown for locating the task.",
    podHeader: "Pod",
    containerHeader: "Container",
    allocateStage: "Node allocation",
    cacheView: "Volcano cache node view",
    resourceFreeTotal: "resources are free / total",
    nodeHeader: "Node",
    conclusionHeader: "Result",
    skippedStage: "Stage not executed",
    previousFailed: "The previous stage did not pass",
    statusPass: "Definitive pass",
    statusFail: "Definitive fail",
    statusUnknown: "Insufficient evidence",
    statusSkipped: "Disabled / not applicable / not reached",
    freeTotalLegend: "Volcano Idle / Allocatable",
    findings: (count) => `Failures and pending confirmations (${count})`,
    policyDetails: "Runtime policy evidence",
    configMap: "ConfigMap",
    actions: "Actions",
    noPlugins: "no plugins",
    notLocated: "not located",
    tiersUnknown: "Plugin tiers unknown",
    llmDetails: "Source and log fallback analysis",
  },
  zh: {
    brandKicker: "VOLCANO 调度诊断",
    brandSubtitle: "沿真实调度顺序定位 Pending：入队 → JobValid → 节点分配",
    reload: "刷新目标",
    analyze: "开始分析",
    podLabel: "故障 Pod",
    branchLabel: "Volcano 分支",
    loading: "正在加载",
    loadingPods: "正在读取 Pending Pod…",
    podsLoadFailed: "Pending Pod 读取失败",
    noPods: "没有 Pending Pod",
    noPodsStatus: "当前集群没有可分析的 Pending Pod",
    podsFound: (count) => `已发现 ${count} 个 Pending Pod`,
    loadingBranches: "正在读取远程分支…",
    branchesLoadFailed: "Volcano 分支读取失败",
    noBranches: "没有可用的远程分支",
    noBranchesStatus: "仓库中没有可供选择的远程分支",
    branchesFound: (count, note) => `已加载 ${count} 个远程分支${note}`,
    schedulerDefaultBranch: (version, branch) => `，scheduler ${version} 默认 ${branch}`,
    schedulerVersionFailed: (message) => `，版本读取失败：${message}`,
    invalidJSON: (status) => `服务返回了无效响应（HTTP ${status}）`,
    selectTarget: "请先选择故障 Pod 和 Volcano 分支",
    analyzeFailed: "分析失败",
    analyzeStatus: (branch) => `正在基于 ${branch} 分支读取 Volcano 调度证据…`,
    progressStages: [
      "前置与工作负载检查",
      "队列、PodGroup 与入队规则",
      "Volcano cache 与节点过滤",
      "规则仍不确定时进入源码、日志与大模型分析",
    ],
    progressHint: "当前分析结束后会刷新报告。",
    waiting: "等待分析结果",
    summarySmall: "SUMMARY",
    summaryTitle: "总结论",
    rootCause: "根因",
    suggestions: "建议",
    noConclusion: "尚未形成结论",
    enqueueStage: "入队检查",
    queueRuntime: "队列运行时资源与 JobEnqueueable",
    runtimeUnavailable: "运行时快照不可用",
    unknownStrategy: "未确定",
    queueHeader: "队列",
    strategyHeader: "策略",
    cap: "realCapability",
    alloc: "已分配",
    inq: "已入队",
    req: "队列请求",
    cand: "当前 PodGroup MinResources",
    elastic: "弹性资源",
    avail: "Cap−Alloc−InQ+Elastic",
    need: "Cand+Alloc+InQ−Elastic",
    queueFormula: "入队公式",
    proportionFormulaNote: "proportion 入队使用该公式；后续 allocate 按公平份额 deserved − allocated 限制分配。",
    capacityFormulaNote: "capacity 入队使用相同公式；后续 allocate 按 realCapability − allocated 限制，并使用 Queue.spec.deserved 计算份额与排序。",
    noQueueFormulaNote: "未发现已启用的 proportion/capacity JobEnqueueable 规则。",
    podGroupsTitle: "队列中的 PodGroup",
    items: (count) => `${count} 项`,
    podGroupHeader: "PodGroup / Namespace",
    priority: "优先级",
    phase: "状态",
    noPodGroups: "没有可展示的 PodGroup，或 informer 证据不可用",
    resourceLegend: "PodGroup spec.minResources / requests.*",
    resourceColumn: "资源列",
    priorityLegend: "PriorityClass 数值 / 名称",
    minLegend: "minMember",
    jobValidStage: "作业合法性（allocate 内 JobValid）",
    jobValidNote: "JobValid 接收整个 Job/PodGroup；表中 Container 仅用于定位任务。",
    podHeader: "Pod",
    containerHeader: "Container",
    allocateStage: "节点分配",
    cacheView: "Volcano cache 节点视图",
    resourceFreeTotal: "资源为 free / total",
    nodeHeader: "节点",
    conclusionHeader: "结论",
    skippedStage: "该阶段未执行",
    previousFailed: "上一阶段没有通过",
    statusPass: "确定通过",
    statusFail: "确定失败",
    statusUnknown: "证据不足",
    statusSkipped: "禁用 / 不适用 / 未到达",
    freeTotalLegend: "Volcano Idle / Allocatable",
    findings: (count) => `查看失败与待确认项（${count}）`,
    policyDetails: "运行时策略证据",
    configMap: "ConfigMap",
    actions: "Actions",
    noPlugins: "无插件",
    notLocated: "未能定位",
    tiersUnknown: "插件 tiers 未知",
    llmDetails: "源码与日志兜底分析",
  },
};

function t(key, ...args) {
  const dictionary = I18N[state.language] || I18N.en;
  const value = dictionary[key] ?? I18N.en[key] ?? key;

  return typeof value === "function" ? value(...args) : value;
}

function applyLanguage() {
  document.documentElement.lang = state.language === "zh" ? "zh-CN" : "en";

  document.querySelectorAll("[data-i18n]").forEach((element) => {
    element.textContent = t(element.dataset.i18n);
  });

  $("#language").textContent = state.language === "zh" ? "English" : "中文";

  if (state.report) {
    renderReport(state.report);
  }

  if (!$("#progress").hidden) {
    renderProgress();
  }
}

function toggleLanguage() {
  state.language = state.language === "zh" ? "en" : "zh";
  localStorage.setItem("volens-language", state.language);
  applyLanguage();
}

const CHECK_ABBREVIATIONS = {
  "node.selector": "NS",
  "node.affinity": "NAff",
  "node.schedulable": "Sched",
  "node.ready": "Ready",
  "node.taints": "Taint",
  "node.pod-count": "Pod#",
  "node.ports": "Port",
  "node.pod-affinity": "PAff",
  "node.volume-limits": "VLim",
  "node.volume-zone": "VZone",
  "node.topology-spread": "Spread",
  "node.proportional": "PRes",
  "node.resources": "Res",
  "node.pv-bind": "PVBind",
  "job.queue.exists": "Queue",
  "job.enqueue.evidence": "Enq",
  "queue.runtime-snapshot": "QRes",
  "queue.enqueue-capacity": "QCap",
  "queue.podgroups": "PGs",
  "plugins.gang.min-member": "Gang-Min",
  "plugins.gang.tasks": "Gang-Task",
};

const RESOURCE_ORDER = ["cpu", "memory", "ephemeral-storage", "pods"];

const NODE_CHECK_ORDER = [
  "node.ready",
  "node.resources",
  "node.pod-count",
  "node.schedulable",
  "node.selector",
  "node.affinity",
  "node.taints",
  "node.ports",
  "node.pod-affinity",
  "node.volume-limits",
  "node.volume-zone",
  "node.topology-spread",
  "node.proportional",
  "node.pv-bind",
];

function escapeHTML(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function setSelectMessage(select, message) {
  const option = document.createElement("option");

  option.textContent = message;
  select.replaceChildren(option);
  select.disabled = true;
}

function setFieldStatus(name, message, status) {
  const element = $(`#${name}-status`);

  element.textContent = message;
  element.dataset.state = status;
}

function setGlobalStatus(message, status = "") {
  const element = $("#status");

  element.textContent = message;
  element.dataset.state = status;
}

function syncAnalyzeButton() {
  $("#analyze").disabled = state.analyzing || !state.podsReady || !state.branchesReady;
}

async function responseJSON(response) {
  const text = await response.text();

  if (!text) {
    return {};
  }

  try {
    return JSON.parse(text);
  } catch {
    throw new Error(t("invalidJSON", response.status));
  }
}

async function loadPods() {
  const select = $("#pod");

  state.podsReady = false;
  setSelectMessage(select, t("loadingPods"));
  setFieldStatus("pod", t("loading"), "loading");
  syncAnalyzeButton();

  try {
    const response = await fetch("/api/pods");
    const payload = await responseJSON(response);

    if (!response.ok) {
      throw new Error(payload.error || t("podsLoadFailed"));
    }

    const pods = (Array.isArray(payload) ? payload : payload.pods || []).filter(
      (pod) => pod && pod.namespace && pod.name,
    );

    if (pods.length === 0) {
      setSelectMessage(select, t("noPods"));
      setFieldStatus("pod", t("noPodsStatus"), "empty");

      return;
    }

    const options = pods.map((pod) => {
      const option = document.createElement("option");

      option.value = `${pod.namespace}/${pod.name}`;
      option.textContent = `${pod.namespace} / ${pod.name} (${pod.scheduler || "default"})`;

      return option;
    });

    select.replaceChildren(...options);
    select.disabled = false;
    state.podsReady = true;
    setFieldStatus("pod", t("podsFound", pods.length), "ready");
  } catch (error) {
    setSelectMessage(select, t("podsLoadFailed"));
    setFieldStatus("pod", error.message, "error");
  } finally {
    syncAnalyzeButton();
  }
}

function normalizeBranches(payload) {
  const source = Array.isArray(payload) ? payload : payload.branches || [];
  const current = Array.isArray(payload) ? "" : payload.current || payload.recommendedBranch || "";
  const seen = new Set();

  return source
    .map((item) => {
      if (typeof item === "string") {
        return { name: item, selected: item === current };
      }

      return {
        name: item?.name || item?.branch || item?.ref || "",
        selected: Boolean(item?.selected) || item?.name === current,
      };
    })
    .filter((branch) => {
      if (!branch.name || seen.has(branch.name)) {
        return false;
      }

      seen.add(branch.name);

      return true;
    });
}

async function loadBranches() {
  const select = $("#branch");

  state.branchesReady = false;
  setSelectMessage(select, t("loadingBranches"));
  setFieldStatus("branch", t("loading"), "loading");
  syncAnalyzeButton();

  try {
    const response = await fetch("/api/branches");
    const payload = await responseJSON(response);

    if (!response.ok) {
      throw new Error(payload.error || t("branchesLoadFailed"));
    }

    const branches = normalizeBranches(payload);

    if (branches.length === 0) {
      setSelectMessage(select, t("noBranches"));
      setFieldStatus("branch", t("noBranchesStatus"), "empty");

      return;
    }

    const options = branches.map((branch) => {
      const option = document.createElement("option");

      option.value = branch.name;
      option.textContent = branch.name;
      option.selected = branch.selected;

      return option;
    });

    select.replaceChildren(...options);
    select.disabled = false;
    state.branchesReady = true;

    const version = payload.schedulerVersion?.version || "";
    const recommended = payload.recommendedBranch || "";
    const versionNote = version && recommended
      ? t("schedulerDefaultBranch", version, recommended)
      : payload.schedulerVersionError
        ? t("schedulerVersionFailed", payload.schedulerVersionError)
        : "";

    setFieldStatus("branch", t("branchesFound", branches.length, versionNote), "ready");
  } catch (error) {
    setSelectMessage(select, t("branchesLoadFailed"));
    setFieldStatus("branch", error.message, "error");
  } finally {
    syncAnalyzeButton();
  }
}

function outcomeOfCheck(check = {}) {
  if (check.skipped) {
    return "skipped";
  }

  if (!check.determinate) {
    return "unknown";
  }

  return check.passed ? "pass" : "fail";
}

function statusMark(value, label = "") {
  const outcome = typeof value === "string" ? value : outcomeOfCheck(value);
  const states = {
    pass: ["✓", t("statusPass")],
    fail: ["×", t("statusFail")],
    unknown: ["?", t("statusUnknown")],
    skipped: ["—", t("statusSkipped")],
  };
  const [glyph, text] = states[outcome] || states.unknown;

  return `<span class="status-mark ${outcome}" role="img" aria-label="${escapeHTML(label ? `${label}：${text}` : text)}" title="${escapeHTML(text)}">${glyph}</span>`;
}

function stageHeader(number, title, state = {}) {
  const outcome = state.outcome || "unknown";

  return `
    <div class="stage-heading">
      <span class="stage-number">${number}</span>
      <div>
        <h2>${escapeHTML(title)} ${statusMark(outcome, title)}</h2>
        <p>${escapeHTML(state.skipReason || state.conclusion || t("waiting"))}</p>
      </div>
    </div>
  `;
}

function resourceNames(items, getter) {
  const names = new Set();

  for (const item of items || []) {
    const resources = getter(item) || {};

    Object.keys(resources).forEach((name) => names.add(canonicalResourceName(name)));
  }

  return [...names].sort((left, right) => {
    const leftIndex = RESOURCE_ORDER.indexOf(left);
    const rightIndex = RESOURCE_ORDER.indexOf(right);

    if (leftIndex !== -1 || rightIndex !== -1) {
      return (leftIndex === -1 ? 99 : leftIndex) - (rightIndex === -1 ? 99 : rightIndex);
    }

    return left.localeCompare(right);
  });
}

function podGroupResourceNames(groups) {
  const names = new Set();

  for (const group of groups || []) {
    const resources = group.resources || {};

    Object.keys(resources).forEach((name) => names.add(canonicalResourceName(name)));
  }

  return [...names].sort((left, right) => {
    const leftIndex = RESOURCE_ORDER.indexOf(left);
    const rightIndex = RESOURCE_ORDER.indexOf(right);

    if (leftIndex !== -1 || rightIndex !== -1) {
      return (leftIndex === -1 ? 99 : leftIndex) - (rightIndex === -1 ? 99 : rightIndex);
    }

    return left.localeCompare(right);
  });
}

function canonicalResourceName(name) {
  const trimmed = String(name || "").trim();

  return trimmed.toLowerCase().startsWith("requests.")
    ? trimmed.slice("requests.".length)
    : trimmed;
}

function podGroupResourceValue(resources = {}, name) {
  return resourceValue(resources, name);
}

function resourceValue(resources = {}, name) {
  const requestName = `requests.${name}`;

  if (Object.prototype.hasOwnProperty.call(resources, requestName)) {
    return resources[requestName];
  }

  return resources[name];
}

function resourceLabel(name) {
  if (name.startsWith("requests.")) {
    return resourceLabel(name.slice("requests.".length));
  }

  const labels = {
    cpu: "CPU",
    memory: "MEM",
    "ephemeral-storage": "EPH",
    pods: "PODS",
  };

  if (labels[name]) {
    return labels[name];
  }

  const suffix = name.split("/").at(-1) || name;

  return suffix.length > 12 ? `${suffix.slice(0, 10)}…` : suffix;
}

function formatBytes(value) {
  if (!Number.isFinite(value)) {
    return "?";
  }

  const units = ["B", "Ki", "Mi", "Gi", "Ti", "Pi"];
  let current = Math.abs(value);
  let index = 0;

  while (current >= 1024 && index < units.length - 1) {
    current /= 1024;
    index += 1;
  }

  const signed = value < 0 ? -current : current;

  return `${signed.toFixed(index === 0 || signed >= 100 ? 0 : signed >= 10 ? 1 : 2)}${units[index]}`;
}

function formatResource(name, value) {
  if (name.startsWith("requests.")) {
    return formatResource(name.slice("requests.".length), value);
  }

  if (!Number.isFinite(value)) {
    return "?";
  }

  if (name === "memory" || name === "ephemeral-storage" || name.endsWith("_bytes")) {
    return formatBytes(value);
  }

  if (name === "cpu") {
    return value.toLocaleString(undefined, { maximumFractionDigits: 3 });
  }

  return value.toLocaleString(undefined, { maximumFractionDigits: 3 });
}

function formatNodeResource(name, value, pair = {}) {
  if (Number.isFinite(value)) {
    return formatResource(name, value);
  }

  if (!Number.isFinite(pair.free) && !Number.isFinite(pair.total)) {
    return "0";
  }

  return "?";
}

function queueResourceInsufficient(value = {}) {
  return Number.isFinite(value.available) &&
    Number.isFinite(value.candidate) &&
    value.candidate > value.available;
}

function queueStrategyExplanation(strategy = "") {
  const normalized = strategy.toLowerCase();

  if (normalized.includes("capacity")) {
    return t("capacityFormulaNote");
  }

  if (normalized.includes("proportion")) {
    return t("proportionFormulaNote");
  }

  return t("noQueueFormulaNote");
}

function resourceAmount(name, amount, highlight = false) {
  const className = highlight ? " class=\"insufficient-resource\"" : "";

  return `<b${className}>${formatResource(name, amount)}</b>`;
}

function nodeResourceRequest(node = {}, name) {
  const resourceCheck = (node.checks || []).find((check) => check.id === "node.resources");
  const evidence = resourceCheck?.evidence || {};
  const request = evidence.request || evidence.requestProjection || {};

  return request[name];
}

function nodeResourceInsufficient(node = {}, name, pair = {}) {
	if (pair.insufficient === true) {
		return true;
	}

  const request = nodeResourceRequest(node, name);

  return Number.isFinite(request) && Number.isFinite(pair.free) && pair.free < request;
}

function checkColumns(rows) {
  const columns = [];
  const seen = new Set();

  for (const row of rows || []) {
    for (const check of row.checks || []) {
      if (!check?.id || seen.has(check.id)) {
        continue;
      }

      seen.add(check.id);
      columns.push({ id: check.id, name: check.name || check.id });
    }
  }

  return columns;
}

function checkAbbreviation(column) {
  if (CHECK_ABBREVIATIONS[column.id]) {
    return CHECK_ABBREVIATIONS[column.id];
  }

  if (column.id.startsWith("plugin.")) {
    const plugin = column.id.split(".")[1] || "Plugin";

    return plugin.length > 8 ? `${plugin.slice(0, 7)}…` : plugin;
  }

  const compact = column.name.replaceAll(/[^A-Za-z0-9#]+/g, "");

  return compact.length > 9 ? `${compact.slice(0, 8)}…` : compact || "Check";
}

function checkCell(check, column) {
  if (!check) {
    return `<td class="check-cell">${statusMark("skipped", column.name)}</td>`;
  }

  const source = Array.isArray(check.source) ? check.source.join("\n") : "";
  const detail = [check.reason, source].filter(Boolean).join("\n\n");

  return `<td class="check-cell" title="${escapeHTML(detail)}">${statusMark(check, column.name)}</td>`;
}

function checksLegend(columns) {
  if (!columns.length) {
    return "";
  }

  return `
    <div class="legend check-legend">
      ${columns
        .map(
          (column) => `<span><b>${escapeHTML(checkAbbreviation(column))}</b> ${escapeHTML(column.name)}</span>`,
        )
        .join("")}
    </div>
  `;
}

function sortNodeCheckColumns(columns) {
  return [...columns].sort((left, right) => {
    const rank = (column) => {
      if (column.id === "node.pv-bind") {
        return 1000;
      }

      if (column.id.startsWith("plugin.")) {
        return 900;
      }

      const index = NODE_CHECK_ORDER.indexOf(column.id);

      return index === -1 ? 800 : index;
    };

    return rank(left) - rank(right);
  });
}

function evidenceList(rows) {
  const findings = [];
  const seen = new Set();

  for (const row of rows || []) {
    for (const check of row.checks || []) {
      const outcome = outcomeOfCheck(check);

      if ((outcome !== "fail" && outcome !== "unknown") || seen.has(`${check.id}/${check.reason}`)) {
        continue;
      }

      seen.add(`${check.id}/${check.reason}`);
      findings.push({ ...check, outcome });
    }
  }

  if (!findings.length) {
    return "";
  }

  return `
    <details class="findings">
      <summary>${escapeHTML(t("findings", findings.length))}</summary>
      <div class="finding-list">
        ${findings
          .map(
            (check) => `
              <article>
                ${statusMark(check.outcome, check.name)}
                <div><strong>${escapeHTML(check.name)}</strong><p>${escapeHTML(check.reason)}</p></div>
              </article>
            `,
          )
          .join("")}
      </div>
    </details>
  `;
}

function renderEnqueue(section = {}) {
  const queue = section.queue || {};
  const checks = Array.isArray(section.checks) ? section.checks : [];
  const checkRow = { checks };
  const columns = checkColumns([checkRow]);
  const resources = resourceNames([{ resources: queue.resources || {} }], (item) => item.resources);

  const resourceHeaders = resources
    .map((name) => `<th title="${escapeHTML(name)}">${escapeHTML(resourceLabel(name))}</th>`)
    .join("");
  const resourceCells = resources
    .map((name) => {
      const value = resourceValue(queue.resources, name) || {};
      const insufficient = queueResourceInsufficient(value);
      const lines = [
        ["Avail", value.available],
        ["Cap", value.capability],
        ["Alloc", value.allocated],
        ["InQ", value.inqueue],
        ["Elastic", value.elastic],
        ["Cand", value.candidate],
        ["Need", value.required],
      ];

      return `
        <td class="resource-stack" title="${escapeHTML(name)}">
          ${lines
            .map(
              ([label, amount]) => {
                const highlight = insufficient &&
                  (label === "Avail" || label === "Cand" || label === "Need");

                return `<span><small>${label}</small>${resourceAmount(name, amount, highlight)}</span>`;
              },
            )
            .join("")}
        </td>
      `;
    })
    .join("");
  const checksHTML = columns
    .map((column) => checkCell(checks.find((check) => check.id === column.id), column))
    .join("");

  const podGroups = Array.isArray(section.podGroups) ? section.podGroups : [];
  const podGroupResources = podGroupResourceNames(podGroups);

  return `
    <section class="stage panel">
      ${stageHeader(1, t("enqueueStage"), section.state)}
      <div class="table-label">
        <h3>${escapeHTML(t("queueRuntime"))}</h3>
        <span>${escapeHTML(queue.runtimeSource || queue.runtimeReason || t("runtimeUnavailable"))}</span>
      </div>
      <div class="table-wrap">
        <table class="matrix queue-matrix">
          <thead><tr><th class="sticky-col">${escapeHTML(t("queueHeader"))}</th><th>${escapeHTML(t("strategyHeader"))}</th>${resourceHeaders}${columns
            .map(
              (column) => `<th title="${escapeHTML(column.name)}">${escapeHTML(checkAbbreviation(column))}</th>`,
            )
            .join("")}</tr></thead>
          <tbody><tr><td class="sticky-col"><strong>${escapeHTML(queue.name || "-")}</strong><small>${escapeHTML(queue.state || "")}</small></td><td>${escapeHTML(queue.strategy || t("unknownStrategy"))}</td>${resourceCells}${checksHTML}</tr></tbody>
        </table>
      </div>
      <div class="legend">
        <span><b>Cap</b> ${escapeHTML(t("cap"))}</span><span><b>Alloc</b> ${escapeHTML(t("alloc"))}</span><span><b>InQ</b> ${escapeHTML(t("inq"))}</span><span><b>Elastic</b> ${escapeHTML(t("elastic"))}</span><span><b>Cand</b> ${escapeHTML(t("cand"))}</span><span><b>Need</b> ${escapeHTML(t("need"))}</span><span><b>Avail</b> ${escapeHTML(t("avail"))}</span>
      </div>
      <div class="queue-formula">
        <strong>${escapeHTML(t("queueFormula"))}</strong>
        <code>${escapeHTML(queue.formula || "need = MinReq + allocated + inqueue - elastic; need <= realCapability")}</code>
        <span>${escapeHTML(queueStrategyExplanation(queue.strategy))}</span>
      </div>
      ${checksLegend(columns)}
      ${evidenceList([checkRow])}
      ${podGroupTable(podGroups, podGroupResources)}
    </section>
  `;
}

function podGroupTable(groups, resources) {
  const headers = resources
    .map((name) => `<th title="${escapeHTML(name)}">${escapeHTML(resourceLabel(name))}</th>`)
    .join("");

  const rows = groups
    .map(
      (group) => `
        <tr class="${group.target ? "target-podgroup" : ""}">
          <td class="sticky-col podgroup-name" title="${escapeHTML(`${group.namespace}/${group.name}`)}"><strong>${escapeHTML(group.name)}</strong><small>${escapeHTML(group.namespace)}</small></td>
          ${resources.map((name) => `<td class="mono">${formatResource(name, podGroupResourceValue(group.resources, name))}</td>`).join("")}
          <td>${escapeHTML(group.priority ?? group.priorityClassName ?? "-")}</td>
          <td>${formatAge(group.ageSeconds)}</td>
          <td>${escapeHTML(group.phase || "-")}</td>
          <td>${escapeHTML(group.minMember ?? "-")}</td>
        </tr>
      `,
    )
    .join("");

  return `
    <div class="table-label secondary-label"><h3>${escapeHTML(t("podGroupsTitle"))}</h3><span>${escapeHTML(t("items", groups.length))}</span></div>
    <div class="table-wrap">
      <table class="matrix">
        <thead><tr><th class="sticky-col">${escapeHTML(t("podGroupHeader"))}</th>${headers}<th>${escapeHTML(t("priority"))}</th><th>Age</th><th>${escapeHTML(t("phase"))}</th><th>Min</th></tr></thead>
        <tbody>${rows || `<tr><td colspan="${resources.length + 6}" class="empty-cell">${escapeHTML(t("noPodGroups"))}</td></tr>`}</tbody>
      </table>
    </div>
    <div class="table-legend compact">
      <span><b>${escapeHTML(t("resourceColumn"))}</b> ${escapeHTML(t("resourceLegend"))}</span>
      <span><b>${escapeHTML(t("priority"))}</b> ${escapeHTML(t("priorityLegend"))}</span>
      <span><b>Min</b> ${escapeHTML(t("minLegend"))}</span>
    </div>
  `;
}

function formatAge(seconds) {
  if (!Number.isFinite(seconds) || seconds < 0) {
    return "-";
  }

  const units = [
    [86400, "d"],
    [3600, "h"],
    [60, "m"],
  ];
  let remaining = Math.floor(seconds);
  const parts = [];

  for (const [size, suffix] of units) {
    if (remaining >= size) {
      parts.push(`${Math.floor(remaining / size)}${suffix}`);
      remaining %= size;
    }

    if (parts.length === 2) {
      break;
    }
  }

  if (!parts.length) {
    parts.push(`${remaining}s`);
  }

  return parts.join(" ");
}

function renderJobValid(section = {}) {
  const rows = Array.isArray(section.rows) ? section.rows : [];
  const columns = checkColumns(rows);
  const resources = resourceNames(rows, (row) => row.resources).filter((name) => name !== "pods");

  return `
    <section class="stage panel">
      ${stageHeader(2, t("jobValidStage"), section.state)}
      ${section.state?.outcome === "skipped" ? skippedPanel(section.state) : `
        <div class="table-wrap">
          <table class="matrix">
            <thead><tr><th class="sticky-col">${escapeHTML(t("podHeader"))}</th><th class="sticky-col second-col">${escapeHTML(t("containerHeader"))}</th>${resources
              .map((name) => `<th title="${escapeHTML(name)}">${escapeHTML(resourceLabel(name))}</th>`)
              .join("")}${columns
              .map(
                (column) => `<th title="${escapeHTML(column.name)}">${escapeHTML(checkAbbreviation(column))}</th>`,
              )
              .join("")}</tr></thead>
            <tbody>${rows
              .map(
                (row) => `<tr><td class="sticky-col"><strong>${escapeHTML(row.pod)}</strong><small>${escapeHTML(row.namespace)}</small></td><td class="sticky-col second-col">${escapeHTML((row.containers || []).join(", ") || "-")}</td>${resources
                  .map((name) => `<td class="mono">${formatResource(name, resourceValue(row.resources, name))}</td>`)
                  .join("")}${columns
                  .map((column) => checkCell((row.checks || []).find((check) => check.id === column.id), column))
                  .join("")}</tr>`,
              )
              .join("")}</tbody>
          </table>
        </div>
        <p class="scope-note">${escapeHTML(t("jobValidNote"))}</p>
        ${checksLegend(columns)}
        ${evidenceList(rows)}
      `}
    </section>
  `;
}

function renderAllocate(section = {}) {
  const nodes = Array.isArray(section.nodes) ? section.nodes : [];

  if (section.state?.outcome === "skipped") {
    return `<section class="stage panel">${stageHeader(3, t("allocateStage"), section.state)}${skippedPanel(section.state)}</section>`;
  }

  const resources = resourceNames(nodes, (node) => node.resources);
  const columns = sortNodeCheckColumns(checkColumns(nodes));

  return `
    <section class="stage panel">
      ${stageHeader(3, t("allocateStage"), section.state)}
      <div class="table-label"><h3>${escapeHTML(t("cacheView"))}</h3><span>${escapeHTML(t("resourceFreeTotal"))}</span></div>
      <div class="table-wrap node-table-wrap">
        <table class="matrix node-matrix">
          <thead><tr><th class="sticky-col">${escapeHTML(t("nodeHeader"))}</th>${resources
            .map((name) => `<th title="${escapeHTML(name)}">${escapeHTML(resourceLabel(name))}</th>`)
            .join("")}${columns
            .map(
              (column) => `<th title="${escapeHTML(column.name)}">${escapeHTML(checkAbbreviation(column))}</th>`,
            )
            .join("")}<th>${escapeHTML(t("conclusionHeader"))}</th></tr></thead>
          <tbody>${nodes
            .map(
              (node) => `<tr><td class="sticky-col"><strong>${escapeHTML(node.name)}</strong></td>${resources
                .map((name) => {
                  const pair = resourceValue(node.resources, name) || {};
                  const insufficient = nodeResourceInsufficient(node, name, pair);
                  const tooltip = `used=${formatResource(name, pair.used)} releasing=${formatResource(name, pair.releasing)}`;

                  return `<td class="mono resource-pair" title="${escapeHTML(tooltip)}"><span class="free ${insufficient ? "insufficient-resource" : ""}">${formatNodeResource(name, pair.free, pair)}</span><i>/</i><span>${formatNodeResource(name, pair.total, pair)}</span></td>`;
                })
                .join("")}${columns
                .map((column) => checkCell((node.checks || []).find((check) => check.id === column.id), column))
                .join("")}<td class="check-cell">${statusMark(node.determinate ? (node.passed ? "pass" : "fail") : "unknown", `${node.name} ${t("conclusionHeader")}`)}</td></tr>`,
            )
            .join("")}</tbody>
        </table>
      </div>
      <div class="legend status-legend">
        <span>${statusMark("pass")} ${escapeHTML(t("statusPass"))}</span><span>${statusMark("fail")} ${escapeHTML(t("statusFail"))}</span><span>${statusMark("unknown")} ${escapeHTML(t("statusUnknown"))}</span><span>${statusMark("skipped")} ${escapeHTML(t("statusSkipped"))}</span><span><b>free / total</b> ${escapeHTML(t("freeTotalLegend"))}</span>
      </div>
      ${checksLegend(columns)}
      ${evidenceList(nodes)}
    </section>
  `;
}

function skippedPanel(state = {}) {
  return `<div class="skipped-panel">${statusMark("skipped")}<div><strong>${escapeHTML(t("skippedStage"))}</strong><p>${escapeHTML(state.skipReason || state.conclusion || t("previousFailed"))}</p></div></div>`;
}

function policyDetails(policy = {}) {
  const actions = Array.isArray(policy.actions) ? policy.actions.join(" → ") : t("unknownStrategy");
  const tiers = (policy.tiers || [])
    .map(
      (tier) => `Tier ${tier.order + 1}: ${(tier.plugins || []).map((plugin) => plugin.name).join(", ") || t("noPlugins")}`,
    )
    .join("\n");
  const configMap = policy.configMapName
    ? `${policy.configMapNamespace}/${policy.configMapName}:${policy.configMapKey || "?"}`
    : t("notLocated");

  return `
    <details class="panel runtime-details">
      <summary>${escapeHTML(t("policyDetails"))}</summary>
      <dl><div><dt>${escapeHTML(t("configMap"))}</dt><dd>${escapeHTML(configMap)}</dd></div><div><dt>ResourceVersion</dt><dd>${escapeHTML(policy.configMapResourceVersion || "-")}</dd></div><div><dt>${escapeHTML(t("actions"))}</dt><dd>${escapeHTML(actions)}</dd></div></dl>
      <pre>${escapeHTML(tiers || t("tiersUnknown"))}</pre>
    </details>
  `;
}

function clearReport() {
  state.report = null;
  $("#summary").hidden = true;
  $("#summary").innerHTML = "";
  $("#enqueue").innerHTML = "";
  $("#job-valid").innerHTML = "";
  $("#allocate").innerHTML = "";
  $("#details").innerHTML = "";
  $("#llm").innerHTML = "";
}

function progressMessage() {
  const stages = t("progressStages");
  const stage = stages[Math.min(state.progressStage, stages.length - 1)];

  return [stage, t("progressHint")];
}

function renderProgress() {
  const progress = $("#progress");
  const [stage, hint] = progressMessage();

  progress.hidden = false;
  progress.className = "analysis-progress panel";
  progress.innerHTML = `
    <div class="progress-title">
      <span></span>
      <strong>${escapeHTML(stage)}<small>${escapeHTML(hint)}</small></strong>
    </div>
    <div class="progress-track"><i></i></div>
  `;
}

function startProgress() {
  stopProgress();

  state.progressStage = 0;
  renderProgress();
  state.progressTimer = window.setInterval(() => {
    const stages = t("progressStages");

    state.progressStage = Math.min(state.progressStage + 1, stages.length - 1);
    renderProgress();
  }, 2400);
}

function stopProgress() {
  if (state.progressTimer) {
    window.clearInterval(state.progressTimer);
    state.progressTimer = null;
  }
}

function hideProgress() {
  const progress = $("#progress");

  stopProgress();
  progress.hidden = true;
  progress.innerHTML = "";
}

function shortText(value, limit = 260) {
  const text = String(value || "")
    .replaceAll(/```[\s\S]*?```/g, " ")
    .replaceAll(/[#*_`>]+/g, "")
    .replaceAll(/\s+/g, " ")
    .trim();

  if (text.length <= limit) {
    return text;
  }

  return `${text.slice(0, limit - 1).trim()}…`;
}

function renderReport(report) {
  state.report = report;

  const summary = $("#summary");
  const diagnosis = report.diagnosis || {};
  const suggestions = Array.isArray(diagnosis.suggestions) ? diagnosis.suggestions : [];
  const deterministicFailure = (report.checks || []).some(
    (check) => check?.determinate && !check?.passed && !check?.skipped,
  );
  const stageOutcomes = [
    report.enqueue?.state?.outcome,
    report.jobValid?.state?.outcome,
    report.allocate?.state?.outcome,
  ].filter((outcome) => outcome && outcome !== "skipped");
  const overallOutcome = deterministicFailure || stageOutcomes.includes("fail")
    ? "fail"
    : report.passed && stageOutcomes.length > 0 && stageOutcomes.every((outcome) => outcome === "pass")
      ? "pass"
      : "unknown";

  summary.hidden = false;
  summary.className = `summary panel ${overallOutcome}`;
  const rootCause = shortText(diagnosis.rootCause || report.conclusion || t("noConclusion"));
  summary.innerHTML = `
    <div class="summary-title">${statusMark(overallOutcome, t("summaryTitle"))}<div><small>${escapeHTML(t("summarySmall"))}</small><h2>${escapeHTML(t("summaryTitle"))}</h2></div></div>
    <div class="diagnosis-grid">
      <article><span>${escapeHTML(t("rootCause"))}</span><strong>${escapeHTML(rootCause)}</strong></article>
      <article><span>${escapeHTML(t("suggestions"))}</span>${suggestions.length ? `<ul>${suggestions.slice(0, 3).map((item) => `<li>${escapeHTML(shortText(item, 120))}</li>`).join("")}</ul>` : `<p>${escapeHTML(shortText(report.conclusion || "-", 120))}</p>`}</article>
    </div>
  `;

  $("#enqueue").innerHTML = renderEnqueue(report.enqueue || {});
  $("#job-valid").innerHTML = renderJobValid(report.jobValid || {});
  $("#allocate").innerHTML = renderAllocate(report.allocate || {});
  $("#details").innerHTML = policyDetails(report.policy || {});
  $("#llm").innerHTML = report.llm
    ? `<details class="panel runtime-details"><summary>${escapeHTML(t("llmDetails"))}</summary><pre>${escapeHTML(report.llm)}</pre></details>`
    : "";
}

async function analyze() {
  const [namespace, pod] = $("#pod").value.split("/");
  const branch = $("#branch").value;

  if (!namespace || !pod || !branch) {
    setGlobalStatus(t("selectTarget"), "error");

    return;
  }

  setGlobalStatus(t("analyzeStatus", branch), "loading");
  clearReport();
  startProgress();
  state.analyzing = true;
  syncAnalyzeButton();

  try {
    const response = await fetch("/api/analyze", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ namespace, pod, branch }),
    });
    const report = await responseJSON(response);

    if (!response.ok) {
      throw new Error(report.error || t("analyzeFailed"));
    }

    setGlobalStatus("");
    hideProgress();
    renderReport(report);
  } catch (error) {
    hideProgress();
    setGlobalStatus(error.message, "error");
  } finally {
    state.analyzing = false;
    syncAnalyzeButton();
  }
}

async function loadAll() {
  const reload = $("#reload");

  reload.disabled = true;
  setGlobalStatus("");

  try {
    await Promise.all([loadPods(), loadBranches()]);
  } finally {
    reload.disabled = false;
  }
}

$("#reload").addEventListener("click", loadAll);
$("#analyze").addEventListener("click", analyze);
$("#language").addEventListener("click", toggleLanguage);

applyLanguage();
loadAll();
