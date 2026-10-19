"use strict";
document.documentElement.classList.add("js");

const menu = document.querySelector(".menu-toggle");
const sidebar = document.querySelector("#sidebar");
menu.addEventListener("click", () => {
  const open = menu.getAttribute("aria-expanded") !== "true";
  menu.setAttribute("aria-expanded", String(open));
  sidebar.classList.toggle("is-open", open);
});
document.addEventListener("keydown", (event) => {
  if (event.key === "Escape") {
    menu.setAttribute("aria-expanded", "false");
    sidebar.classList.remove("is-open");
  }
});

for (const block of document.querySelectorAll("pre:not(.signature)")) {
  const code = block.querySelector("code");
  if (!code || !navigator.clipboard) continue;
  const button = document.createElement("button");
  button.type = "button";
  button.className = "copy-button";
  button.textContent = "Copy";
  button.setAttribute("aria-label", "Copy code example");
  button.addEventListener("click", async () => {
    try {
      await navigator.clipboard.writeText(code.textContent);
      button.textContent = "Copied";
    } catch {
      button.textContent = "Select code";
      const range = document.createRange();
      range.selectNodeContents(code);
      const selection = window.getSelection();
      selection.removeAllRanges();
      selection.addRange(range);
    }
    window.setTimeout(() => { button.textContent = "Copy"; }, 1800);
  });
  block.append(button);
}

const search = document.querySelector("#doc-search");
const results = document.querySelector("#search-results");
let indexPromise;
let request = 0;
search.form.addEventListener("submit", (event) => event.preventDefault());
search.addEventListener("input", async () => {
  const current = ++request;
  const query = search.value.trim().toLowerCase();
  results.replaceChildren();
  results.hidden = !query;
  if (!query) return;
  try {
    indexPromise ??= fetch("search.json").then((response) => {
      if (!response.ok) throw new Error("Search unavailable");
      return response.json();
    });
    const index = await indexPromise;
    if (current !== request) return;
    const terms = query.split(/\s+/);
    const matches = index.filter((entry) => terms.every((term) =>
      (entry.title + " " + entry.text).toLowerCase().includes(term))).sort((a, b) =>
      Number(b.title.toLowerCase().includes(query)) - Number(a.title.toLowerCase().includes(query))).slice(0, 7);
    for (const entry of matches) {
      const link = document.createElement("a");
      link.href = entry.url;
      link.textContent = entry.title;
      results.append(link);
    }
    if (!matches.length) {
      const empty = document.createElement("p");
      empty.textContent = "No matching pages.";
      results.append(empty);
    }
  } catch {
    indexPromise = undefined;
    const fallback = document.createElement("a");
    fallback.href = "api.html";
    fallback.textContent = "Browse the Python API";
    results.replaceChildren(fallback);
  }
});
