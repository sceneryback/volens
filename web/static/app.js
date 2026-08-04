"use strict";

const $ = (selector) => document.querySelector(selector);

const state = {
  analyzing: false,
  branchesReady: false,
  podsReady: false,
};

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
    throw new Error(`服务返回了无效响应（HTTP ${response.status}）`);
  }
}

async function loadPods() {
  const select = $("#pod");

  state.podsReady = false;
  setSelectMessage(select, "正在读取 Pending Pod…");
  setFieldStatus("pod", "正在加载", "loading");
  syncAnalyzeButton();

  try {
    const response = await fetch("/api/pods");
    const payload = await responseJSON(response);

    if (!response.ok) {
      throw new Error(payload.error || "读取 Pending Pod 失败");
    }

    const pods = (Array.isArray(payload) ? payload : payload.pods || []).filter(
      (pod) => pod && pod.namespace && pod.name,
    );

    if (pods.length === 0) {
      setSelectMessage(select, "没有 Pending Pod");
      setFieldStatus("pod", "当前集群没有可分析的 Pending Pod", "empty");

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
    setFieldStatus("pod", `已发现 ${pods.length} 个 Pending Pod`, "ready");
  } catch (error) {
    setSelectMessage(select, "Pending Pod 读取失败");
    setFieldStatus("pod", error.message, "error");
  } finally {
    syncAnalyzeButton();
  }
}

function normalizeBranches(payload) {
  const source = Array.isArray(payload) ? payload : payload.branches || [];
  const current = Array.isArray(payload) ? "" : payload.current || "";
  const seen = new Set();

  const branches = source
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

  return branches;
}

async function loadBranches() {
  const select = $("#branch");

  state.branchesReady = false;
  setSelectMessage(select, "正在读取远程分支…");
  setFieldStatus("branch", "正在加载", "loading");
  syncAnalyzeButton();

  try {
    const response = await fetch("/api/branches");
    const payload = await responseJSON(response);

    if (!response.ok) {
      throw new Error(payload.error || "读取 Volcano 分支失败");
    }

    const branches = normalizeBranches(payload);

    if (branches.length === 0) {
      setSelectMessage(select, "没有可用的远程分支");
      setFieldStatus("branch", "仓库中没有可供选择的远程分支", "empty");

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
    setFieldStatus("branch", `已加载 ${branches.length} 个远程分支`, "ready");
  } catch (error) {
    setSelectMessage(select, "Volcano 分支读取失败");
    setFieldStatus("branch", error.message, "error");
  } finally {
    syncAnalyzeButton();
  }
}

function badge(passed, determinate = true) {
  const className = passed ? "pass" : "fail";
  const suffix = determinate ? "" : " · 未确定";

  return `<span class="pill ${className}">${Boolean(passed)}${suffix}</span>`;
}

function checkRows(checks = []) {
  return checks
    .map(
      (check) => `
        <div class="card row">
          <span>${escapeHTML(check.stage)}</span>
          ${badge(check.passed, check.determinate)}
          <strong>${escapeHTML(check.name)}</strong>
          <span>${escapeHTML(check.reason)}</span>
        </div>
      `,
    )
    .join("");
}

function nodeRows(nodes = []) {
  return nodes
    .map((node) => {
      const checks = (node.checks || [])
        .map(
          (check) =>
            `<td title="${escapeHTML(check.reason)}">${badge(check.passed, check.determinate)}</td>`,
        )
        .join("");

      return `
        <tr>
          <td>${escapeHTML(node.name)}</td>
          <td>${badge(node.passed, node.determinate)}</td>
          ${checks}
        </tr>
      `;
    })
    .join("");
}

function renderReport(report) {
  const summary = $("#summary");

  summary.hidden = false;
  summary.className = `summary ${report.passed ? "summary-pass" : "summary-fail"}`;
  summary.innerHTML = `
    <h2>${badge(report.passed)} 总结论</h2>
    <pre>${escapeHTML(report.conclusion)}</pre>
  `;

  $("#checks").innerHTML = `<h2>调度链检查</h2>${checkRows(report.checks)}`;
  $("#nodes").innerHTML = `
    <h2>节点过滤</h2>
    <div class="nodes">
      <table>
        <thead>
          <tr>
            <th>节点</th>
            <th>总结果</th>
            <th>selector</th>
            <th>affinity</th>
            <th>schedulable</th>
            <th>ready</th>
            <th>taints</th>
            <th>Volcano idle</th>
          </tr>
        </thead>
        <tbody>${nodeRows(report.nodes)}</tbody>
      </table>
    </div>
  `;
  $("#llm").innerHTML = report.llm
    ? `<h2>源码兜底分析</h2><pre class="card">${escapeHTML(report.llm)}</pre>`
    : "";
}

async function analyze() {
  const [namespace, pod] = $("#pod").value.split("/");
  const branch = $("#branch").value;

  if (!namespace || !pod || !branch) {
    setGlobalStatus("请先选择故障 Pod 和 Volcano 分支", "error");

    return;
  }

  setGlobalStatus(`正在基于 ${branch} 分支读取 Volcano 调度证据…`, "loading");
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
      throw new Error(report.error || "分析失败");
    }

    setGlobalStatus("");
    renderReport(report);
  } catch (error) {
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

loadAll();
