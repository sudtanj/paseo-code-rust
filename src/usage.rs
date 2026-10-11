//! Token and cost accounting per provider and model (`usage.list_reports`).
//!
//! The CLIs report running totals: Claude Code's `result` carries the
//! session's `modelUsage` (tokens and USD per model, restored on `--resume`),
//! Codex's `thread/tokenUsage/updated` the thread's token totals. Each agent
//! keeps a cursor at the last totals it saw, so only the growth since then is
//! booked. Totals that went down (new session, fresh process) start over.
//!
//! Spend is kept per UTC day, provider and model in `usage.json`. Codex
//! reports no prices, so its cost (and Claude's for a model it couldn't price)
//! is estimated from LiteLLM's public price list, refreshed daily into
//! `model-prices.json`. Costs are API list prices: what the same tokens would
//! cost through the API, not what a subscription is billed.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

/// Spend older than this is dropped.
const KEEP_DAYS: u64 = 400;
const PRICES_HOST: &str = "raw.githubusercontent.com";
const PRICES_PATH: &str = "/BerriAI/litellm/main/model_prices_and_context_window.json";
const PRICES_MAX_AGE: Duration = Duration::from_secs(24 * 3600);

#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Counters {
    /// Input tokens not served from the prompt cache.
    pub input: u64,
    /// Input tokens read from the prompt cache.
    pub cached: u64,
    /// Input tokens written to the prompt cache (Claude only).
    pub cache_write: u64,
    pub output: u64,
    /// USD at API list prices.
    pub cost: f64,
}

impl Counters {
    pub fn tokens(&self) -> u64 {
        self.input + self.cached + self.cache_write + self.output
    }

    fn is_zero(&self) -> bool {
        self.tokens() == 0 && self.cost <= 0.0
    }

    fn add(&mut self, o: &Counters) {
        self.input += o.input;
        self.cached += o.cached;
        self.cache_write += o.cache_write;
        self.output += o.output;
        self.cost += o.cost;
    }

    /// The growth from `prev` to `self`, or None if any total went down
    /// (the counters started over).
    fn since(&self, prev: &Counters) -> Option<Counters> {
        if self.input < prev.input || self.cached < prev.cached || self.cache_write < prev.cache_write || self.output < prev.output || self.cost + 1e-9 < prev.cost {
            return None;
        }
        Some(Counters {
            input: self.input - prev.input,
            cached: self.cached - prev.cached,
            cache_write: self.cache_write - prev.cache_write,
            output: self.output - prev.output,
            cost: (self.cost - prev.cost).max(0.0),
        })
    }
}

/// Running totals reported by a CLI.
#[derive(Debug)]
pub enum Sample {
    /// Claude Code: the session's totals per model (`modelUsage` of a `result`).
    Claude { session: String, models: BTreeMap<String, Counters> },
    /// Codex: the thread's token totals (no prices, no per-model split).
    Codex { thread: String, totals: Counters },
}

/// The running totals in a Claude Code `result` message.
pub fn claude_sample(result: &Value) -> Option<Sample> {
    let session = result.get("session_id")?.as_str()?.to_string();
    let models = result.get("modelUsage")?.as_object()?;
    let n = |m: &Value, k: &str| m.get(k).and_then(Value::as_u64).unwrap_or(0);
    let models = models
        .iter()
        .map(|(name, m)| {
            let c = Counters {
                input: n(m, "inputTokens"),
                cached: n(m, "cacheReadInputTokens"),
                cache_write: n(m, "cacheCreationInputTokens"),
                output: n(m, "outputTokens"),
                cost: m.get("costUSD").and_then(Value::as_f64).unwrap_or(0.0),
            };
            (name.clone(), c)
        })
        .collect();
    Some(Sample::Claude { session, models })
}

/// The running totals in Codex's `tokenUsage.total` (input includes cached).
pub fn codex_sample(thread: &str, total: &Value) -> Option<Sample> {
    let n = |a: &str, b: &str| total.get(a).or_else(|| total.get(b)).and_then(Value::as_u64);
    let input = n("inputTokens", "input_tokens")?;
    let cached = n("cachedInputTokens", "cached_input_tokens").unwrap_or(0).min(input);
    let output = n("outputTokens", "output_tokens").unwrap_or(0);
    Some(Sample::Codex { thread: thread.to_string(), totals: Counters { input: input - cached, cached, cache_write: 0, output, cost: 0.0 } })
}

/// USD per token.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct Price {
    pub input: f64,
    pub output: f64,
    pub cache_read: f64,
    pub cache_write: f64,
}

impl Price {
    fn cost(&self, c: &Counters) -> f64 {
        c.input as f64 * self.input + c.cached as f64 * self.cache_read + c.cache_write as f64 * self.cache_write + c.output as f64 * self.output
    }
}

static PRICES: Mutex<Option<HashMap<String, Price>>> = Mutex::new(None);

fn prices_path(home: &Path) -> PathBuf {
    home.join("model-prices.json")
}

/// Load the saved price list, if any.
pub fn load_prices(home: &Path) {
    let loaded = std::fs::read(prices_path(home)).ok().and_then(|b| serde_json::from_slice::<HashMap<String, Price>>(&b).ok());
    if let Some(p) = loaded {
        *PRICES.lock().unwrap() = Some(p);
    }
}

/// The price of `model`, matched loosely: `claude-opus-5-5[1m]`,
/// `claude-opus-5-5-20260101` and `openai/gpt-5.5` all find their base entry.
pub fn price(model: &str) -> Option<Price> {
    lookup(PRICES.lock().unwrap().as_ref()?, model)
}

fn lookup(prices: &HashMap<String, Price>, model: &str) -> Option<Price> {
    let mut name = model.trim().to_ascii_lowercase();
    if let Some(i) = name.find('[') {
        name.truncate(i);
    }
    if let Some((_, rest)) = name.rsplit_once('/') {
        name = rest.to_string();
    }
    if let Some(p) = prices.get(&name) {
        return Some(*p);
    }
    // Drop a date suffix: -20260101 or -2026-01-01.
    let parts: Vec<&str> = name.split('-').collect();
    for cut in [1, 3] {
        if parts.len() > cut && parts[parts.len() - cut..].iter().all(|p| p.chars().all(|c| c.is_ascii_digit())) && parts[parts.len() - cut..].concat().len() == 8 {
            if let Some(p) = prices.get(&parts[..parts.len() - cut].join("-")) {
                return Some(*p);
            }
        }
    }
    None
}

#[derive(Deserialize)]
struct RawPrice {
    #[serde(default)]
    litellm_provider: Option<Value>,
    #[serde(default)]
    input_cost_per_token: Option<Value>,
    #[serde(default)]
    output_cost_per_token: Option<Value>,
    #[serde(default)]
    cache_read_input_token_cost: Option<Value>,
    #[serde(default)]
    cache_creation_input_token_cost: Option<Value>,
}

/// Keep the Anthropic and OpenAI chat models of LiteLLM's price list.
fn parse_litellm(body: &str) -> Result<HashMap<String, Price>, String> {
    let raw: HashMap<String, RawPrice> = serde_json::from_str(body).map_err(|e| e.to_string())?;
    let f = |v: &Option<Value>| v.as_ref().and_then(Value::as_f64);
    let mut out = HashMap::new();
    for (name, p) in raw {
        let provider = p.litellm_provider.as_ref().and_then(Value::as_str).unwrap_or("");
        if name.contains('/') || !matches!(provider, "anthropic" | "openai") {
            continue;
        }
        let (Some(input), Some(output)) = (f(&p.input_cost_per_token), f(&p.output_cost_per_token)) else { continue };
        let price = Price {
            input,
            output,
            cache_read: f(&p.cache_read_input_token_cost).unwrap_or(input * 0.1),
            cache_write: f(&p.cache_creation_input_token_cost).unwrap_or(input * 1.25),
        };
        out.insert(name.to_ascii_lowercase(), price);
    }
    if out.is_empty() {
        return Err("no Anthropic or OpenAI prices in the list".into());
    }
    Ok(out)
}

/// Keep `model-prices.json` at most a day old, for as long as the daemon runs.
pub async fn refresh_prices(home: PathBuf) {
    loop {
        let age = std::fs::metadata(prices_path(&home)).and_then(|m| m.modified()).ok().and_then(|t| SystemTime::now().duration_since(t).ok());
        if age.map(|a| a >= PRICES_MAX_AGE).unwrap_or(true) {
            match fetch_prices().await {
                Ok(prices) => {
                    let bytes = serde_json::to_vec(&prices).unwrap_or_default();
                    if let Err(e) = crate::util::write_atomic(&prices_path(&home), &bytes, false) {
                        crate::warn!("could not save model prices: {e}");
                    }
                    crate::log!("loaded prices for {} models", prices.len());
                    *PRICES.lock().unwrap() = Some(prices);
                }
                Err(e) => crate::warn!("could not fetch model prices: {e}"),
            }
        }
        tokio::time::sleep(Duration::from_secs(6 * 3600)).await;
    }
}

async fn fetch_prices() -> Result<HashMap<String, Price>, String> {
    let (status, body) = crate::push::get(PRICES_HOST, PRICES_PATH, 16 * 1024 * 1024, Duration::from_secs(90)).await?;
    if status != 200 {
        return Err(format!("HTTP {status}"));
    }
    parse_litellm(&body)
}

/// UTC date (`YYYY-MM-DD`) `days_ago` days before now.
fn day(days_ago: u64) -> String {
    let t = SystemTime::now() - Duration::from_secs(days_ago * 24 * 3600);
    humantime::format_rfc3339_seconds(t).to_string()[..10].to_string()
}

#[derive(Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct AgentLedger {
    /// Session (Claude) or thread (Codex) the cursor belongs to.
    key: String,
    /// The CLI's running totals at the last sample, per model ("" for Codex).
    cursor: BTreeMap<String, Counters>,
    /// Everything this agent has used.
    total: Counters,
}

#[derive(Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct Data {
    /// UTC day -> provider -> model -> spend.
    days: BTreeMap<String, BTreeMap<String, BTreeMap<String, Counters>>>,
    agents: BTreeMap<String, AgentLedger>,
}

pub struct Ledger {
    path: PathBuf,
    data: Data,
    dirty: bool,
}

impl Ledger {
    pub fn load(home: &Path) -> Ledger {
        let path = home.join("usage.json");
        let data = std::fs::read(&path).ok().and_then(|b| serde_json::from_slice(&b).ok()).unwrap_or_default();
        Ledger { path, data, dirty: false }
    }

    pub fn save_if_dirty(&mut self) {
        if !self.dirty {
            return;
        }
        self.dirty = false;
        let oldest = day(KEEP_DAYS);
        self.data.days.retain(|d, _| *d >= oldest);
        let bytes = serde_json::to_vec(&self.data).unwrap_or_default();
        if let Err(e) = crate::util::write_atomic(&self.path, &bytes, false) {
            crate::warn!("could not save usage: {e}");
        }
    }

    /// Book the growth since the agent's last sample and return it.
    /// `model` names Codex spend (it reports no model).
    pub fn record(&mut self, agent_id: &str, provider: &str, model: &str, sample: Sample) -> Counters {
        let (key, current) = match sample {
            Sample::Claude { session, models } => (session, models),
            Sample::Codex { thread, totals } => (thread, BTreeMap::from([(String::new(), totals)])),
        };
        let data = &mut self.data;
        let agent = data.agents.entry(agent_id.to_string()).or_default();
        let same = agent.key == key;
        let models = data.days.entry(day(0)).or_default().entry(provider.to_string()).or_default();
        let mut spent = Counters::default();
        for (name, cur) in &current {
            let prev = agent.cursor.get(name).filter(|_| same).copied().unwrap_or_default();
            let mut delta = cur.since(&prev).unwrap_or(*cur);
            if delta.is_zero() {
                continue;
            }
            let label = if name.is_empty() { model } else { name.as_str() };
            if delta.cost <= 0.0 {
                delta.cost = price(label).map(|p| p.cost(&delta)).unwrap_or(0.0);
            }
            models.entry(label.to_string()).or_default().add(&delta);
            spent.add(&delta);
        }
        agent.key = key;
        agent.cursor = current;
        agent.total.add(&spent);
        self.dirty = true;
        spent
    }

    /// Everything `agent_id` has used.
    pub fn agent_total(&self, agent_id: &str) -> Option<Counters> {
        self.data.agents.get(agent_id).map(|a| a.total)
    }

    pub fn forget_agent(&mut self, agent_id: &str) {
        if self.data.agents.remove(agent_id).is_some() {
            self.dirty = true;
        }
    }

    /// Spend per model of `provider` over the last `days` days (0 = all time).
    fn per_model(&self, provider: &str, days: u64) -> BTreeMap<String, Counters> {
        let from = if days == 0 { String::new() } else { day(days - 1) };
        let mut out: BTreeMap<String, Counters> = BTreeMap::new();
        for (_, providers) in self.data.days.range(from..) {
            for (model, c) in providers.get(provider).into_iter().flatten() {
                out.entry(model.clone()).or_default().add(c);
            }
        }
        out
    }

    fn sum(&self, provider: &str, days: u64) -> Counters {
        let mut total = Counters::default();
        for c in self.per_model(provider, days).values() {
            total.add(c);
        }
        total
    }

    /// For estimating and trimming cost: the 30-day daily average and monthly
    /// projection, the prompt-cache hit rate, then each day of the last 30
    /// that had spend, newest first.
    fn trend(&self, provider: &str, month: &Counters) -> Vec<Value> {
        let daily: Vec<(String, Counters)> = (0..30)
            .map(day)
            .filter_map(|d| {
                let models = self.data.days.get(&d)?.get(provider)?;
                let mut c = Counters::default();
                models.values().for_each(|m| c.add(m));
                (!c.is_zero()).then_some((d, c))
            })
            .collect();
        let Some((oldest, _)) = daily.last() else { return Vec::new() };
        // Average over the days since tracking began, not a full 30 if newer.
        let span = (0..30).position(|k| day(k) == *oldest).map(|k| k as u64 + 1).unwrap_or(30);
        let avg_cost = month.cost / span as f64;
        let avg_tokens = month.tokens() / span;
        let prompt = month.input + month.cached + month.cache_write;
        let mut out = vec![
            json!({ "id": "avg-day", "label": format!("Daily average ({span}d)"), "value": format!("{} · {} tok", usd(avg_cost), short(avg_tokens)) }),
            json!({ "id": "projected-month", "label": "Projected per 30 days", "value": usd(avg_cost * 30.0) }),
        ];
        if prompt > 0 {
            out.push(json!({ "id": "cache-hit", "label": "Prompt cache hit rate (30d)", "value": format!("{:.0}%", month.cached as f64 * 100.0 / prompt as f64) }));
        }
        for (d, c) in daily {
            out.push(json!({ "id": format!("day-{d}"), "label": d, "value": format!("{} tok · {}", short(c.tokens()), usd(c.cost)) }));
        }
        out
    }

    /// One `UsageReportEntry` for `provider`; for an agent, its own spend first.
    pub fn report(&self, provider: &str, label: &str, agent_id: Option<&str>) -> Value {
        let money = |id: &str, label: &str, usd: f64| json!({ "id": id, "label": label, "used": round_usd(usd), "unit": "usd" });
        let tokens = |id: &str, label: &str, n: u64| json!({ "id": id, "label": label, "used": n, "unit": "tokens" });
        let (today, week, month, all) = (self.sum(provider, 1), self.sum(provider, 7), self.sum(provider, 30), self.sum(provider, 0));
        let mut balances = Vec::new();
        if let Some(id) = agent_id {
            let mine = self.agent_total(id).unwrap_or_default();
            balances.push(money("agent-cost", "This agent: cost", mine.cost));
            balances.push(tokens("agent-tokens", "This agent: tokens", mine.tokens()));
        }
        balances.extend([
            money("cost-today", "Cost today", today.cost),
            money("cost-7d", "Cost, last 7 days", week.cost),
            money("cost-30d", "Cost, last 30 days", month.cost),
            money("cost-all", "Cost, all time", all.cost),
            tokens("tokens-today", "Tokens today", today.tokens()),
            tokens("tokens-30d", "Tokens, last 30 days", month.tokens()),
            tokens("tokens-all", "Tokens, all time", all.tokens()),
        ]);
        let mut details = vec![json!({ "id": "split-30d", "label": "Last 30 days", "value": split(&month) })];
        let mut models: Vec<(String, Counters)> = self.per_model(provider, 30).into_iter().collect();
        models.sort_by(|a, b| b.1.cost.total_cmp(&a.1.cost).then(b.1.tokens().cmp(&a.1.tokens())));
        for (model, c) in models.into_iter().take(8) {
            details.push(json!({ "id": format!("model-{model}"), "label": model, "value": format!("{} tok · {}", short(c.tokens()), usd(c.cost)) }));
        }
        details.extend(self.trend(provider, &month));
        let mut entry = json!({
            "id": provider,
            "sourceId": provider,
            "sourceLabel": label,
            "account": { "label": "API list prices · days in UTC" },
            "fetchedAt": crate::util::now_iso(),
            "report": { "status": "available", "windows": [], "balances": balances, "details": details },
        });
        if let Some(svg) = icon(provider) {
            entry["icon"] = json!(svg);
        }
        entry
    }
}

fn round_usd(v: f64) -> f64 {
    (v * 10_000.0).round() / 10_000.0
}

fn usd(v: f64) -> String {
    if v > 0.0 && v < 0.01 { format!("${v:.4}") } else { format!("${v:.2}") }
}

/// 1234 -> "1.2K", 3_400_000 -> "3.4M".
fn short(n: u64) -> String {
    match n {
        0..=999 => n.to_string(),
        1_000..=999_999 => format!("{:.1}K", n as f64 / 1e3),
        1_000_000..=999_999_999 => format!("{:.1}M", n as f64 / 1e6),
        _ => format!("{:.2}B", n as f64 / 1e9),
    }
}

fn split(c: &Counters) -> String {
    let mut s = format!("in {} · cached {}", short(c.input), short(c.cached));
    if c.cache_write > 0 {
        s += &format!(" · cache write {}", short(c.cache_write));
    }
    s + &format!(" · out {}", short(c.output))
}

fn icon(provider: &str) -> Option<&'static str> {
    match provider {
        "claude" => Some(CLAUDE_ICON),
        "codex" => Some(CODEX_ICON),
        _ => None,
    }
}

const CLAUDE_ICON: &str = r#"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor"><path d="M4.709 15.955l4.72-2.647.08-.23-.08-.128H9.2l-.79-.048-2.698-.073-2.339-.097-2.266-.122-.571-.121L0 11.784l.055-.352.48-.321.686.06 1.52.103 2.278.158 1.652.097 2.449.255h.389l.055-.157-.134-.098-.103-.097-2.358-1.596-2.552-1.688-1.336-.972-.724-.491-.364-.462-.158-1.008.656-.722.881.06.225.061.893.686 1.908 1.476 2.491 1.833.365.304.145-.103.019-.073-.164-.274-1.355-2.446-1.446-2.49-.644-1.032-.17-.619a2.97 2.97 0 01-.104-.729L6.283.134 6.696 0l.996.134.42.364.62 1.414 1.002 2.229 1.555 3.03.456.898.243.832.091.255h.158V9.01l.128-1.706.237-2.095.23-2.695.08-.76.376-.91.747-.492.584.28.48.685-.067.444-.286 1.851-.559 2.903-.364 1.942h.212l.243-.242.985-1.306 1.652-2.064.73-.82.85-.904.547-.431h1.033l.76 1.129-.34 1.166-1.064 1.347-.881 1.142-1.264 1.7-.79 1.36.073.11.188-.02 2.856-.606 1.543-.28 1.841-.315.833.388.091.395-.328.807-1.969.486-2.309.462-3.439.813-.042.03.049.061 1.549.146.662.036h1.622l3.02.225.79.522.474.638-.079.485-1.215.62-1.64-.389-3.829-.91-1.312-.329h-.182v.11l1.093 1.068 2.006 1.81 2.509 2.33.127.578-.322.455-.34-.049-2.205-1.657-.851-.747-1.926-1.62h-.128v.17l.444.649 2.345 3.521.122 1.08-.17.353-.608.213-.668-.122-1.374-1.925-1.415-2.167-1.143-1.943-.14.08-.674 7.254-.316.37-.729.28-.607-.461-.322-.747.322-1.476.389-1.924.315-1.53.286-1.9.17-.632-.012-.042-.14.018-1.434 1.967-2.18 2.945-1.726 1.845-.414.164-.717-.37.067-.662.401-.589 2.388-3.036 1.44-1.882.93-1.086-.006-.158h-.055L4.132 18.56l-1.13.146-.487-.456.061-.746.231-.243 1.908-1.312-.006.006z"/></svg>"#;

const CODEX_ICON: &str = r#"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor" fill-rule="evenodd"><path d="M21.55 10.004a5.416 5.416 0 00-.478-4.501c-1.217-2.09-3.662-3.166-6.05-2.66A5.59 5.59 0 0010.831 1C8.39.995 6.224 2.546 5.473 4.838A5.553 5.553 0 001.76 7.496a5.487 5.487 0 00.691 6.5 5.416 5.416 0 00.477 4.502c1.217 2.09 3.662 3.165 6.05 2.66A5.586 5.586 0 0013.168 23c2.443.006 4.61-1.546 5.361-3.84a5.553 5.553 0 003.715-2.66 5.488 5.488 0 00-.693-6.497v.001zm-8.381 11.558a4.199 4.199 0 01-2.675-.954c.034-.018.093-.05.132-.074l4.44-2.53a.71.71 0 00.364-.623v-6.176l1.877 1.069c.02.01.033.029.036.05v5.115c-.003 2.274-1.87 4.118-4.174 4.123zM4.192 17.78a4.059 4.059 0 01-.498-2.763c.032.02.09.055.131.078l4.44 2.53c.225.13.504.13.73 0l5.42-3.088v2.138a.068.068 0 01-.027.057L9.9 19.288c-1.999 1.136-4.552.46-5.707-1.51h-.001zM3.023 8.216A4.15 4.15 0 015.198 6.41l-.002.151v5.06a.711.711 0 00.364.624l5.42 3.087-1.876 1.07a.067.067 0 01-.063.005l-4.489-2.559c-1.995-1.14-2.679-3.658-1.53-5.63h.001zm15.417 3.54l-5.42-3.088L14.896 7.6a.067.067 0 01.063-.006l4.489 2.557c1.998 1.14 2.683 3.662 1.529 5.633a4.163 4.163 0 01-2.174 1.807V12.38a.71.71 0 00-.363-.623zm1.867-2.773a6.04 6.04 0 00-.132-.078l-4.44-2.53a.731.731 0 00-.729 0l-5.42 3.088V7.325a.068.068 0 01.027-.057L14.1 4.713c2-1.137 4.555-.46 5.707 1.513.487.833.664 1.809.499 2.757h.001zm-11.741 3.81l-1.877-1.068a.065.065 0 01-.036-.051V6.559c.001-2.277 1.873-4.122 4.181-4.12.976 0 1.92.338 2.671.954-.034.018-.092.05-.131.073l-4.44 2.53a.71.71 0 00-.365.623l-.003 6.173v.002zm1.02-2.168L12 9.25l2.414 1.375v2.75L12 14.75l-2.415-1.375v-2.75z"/></svg>"#;

#[cfg(test)]
mod tests {
    use super::*;

    fn claude(session: &str, input: u64, output: u64, cost: f64) -> Sample {
        let r = json!({ "session_id": session, "modelUsage": { "claude-opus-5-5": { "inputTokens": input, "outputTokens": output, "cacheReadInputTokens": 0, "cacheCreationInputTokens": 0, "costUSD": cost } } });
        claude_sample(&r).unwrap()
    }

    fn ledger() -> (Ledger, PathBuf) {
        let dir = std::env::temp_dir().join(format!("paseo-usage-{}", crate::util::new_id()));
        std::fs::create_dir_all(&dir).unwrap();
        (Ledger::load(&dir), dir)
    }

    #[test]
    fn claude_books_growth_and_restarts() {
        let (mut l, dir) = ledger();
        assert_eq!(l.record("a", "claude", "", claude("s1", 100, 10, 0.5)).tokens(), 110);
        // Same session, totals grew: only the growth counts.
        let d = l.record("a", "claude", "", claude("s1", 150, 30, 0.75));
        assert_eq!((d.input, d.output), (50, 20));
        assert!((d.cost - 0.25).abs() < 1e-9);
        // Same totals again: nothing new.
        assert!(l.record("a", "claude", "", claude("s1", 150, 30, 0.75)).is_zero());
        // A new session starts from zero.
        assert_eq!(l.record("a", "claude", "", claude("s2", 20, 5, 0.1)).tokens(), 25);
        // Totals that went down (fresh process) count in full.
        assert_eq!(l.record("a", "claude", "", claude("s2", 7, 1, 0.01)).tokens(), 8);
        let total = l.agent_total("a").unwrap();
        assert_eq!(total.tokens(), 110 + 70 + 25 + 8);
        assert!((total.cost - 0.86).abs() < 1e-9);
        assert_eq!(l.sum("claude", 1).tokens(), total.tokens());
        // Survives a reload.
        l.save_if_dirty();
        let again = Ledger::load(&dir);
        assert_eq!(again.agent_total("a").unwrap().tokens(), total.tokens());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn codex_is_priced_from_the_list() {
        let (mut l, dir) = ledger();
        *PRICES.lock().unwrap() = Some(HashMap::from([("gpt-test".to_string(), Price { input: 1e-6, output: 1e-5, cache_read: 1e-7, cache_write: 1e-6 })]));
        let total = json!({ "inputTokens": 1000, "cachedInputTokens": 400, "outputTokens": 100 });
        let d = l.record("b", "codex", "gpt-test", codex_sample("t1", &total).unwrap());
        assert_eq!((d.input, d.cached, d.output), (600, 400, 100));
        assert!((d.cost - (600e-6 + 400e-7 + 100e-5)).abs() < 1e-12);
        let report = l.report("codex", "Codex", Some("b"));
        assert_eq!(report["report"]["status"], "available");
        assert_eq!(report["report"]["balances"][0]["id"], "agent-cost");
        assert!(report["report"]["details"].as_array().unwrap().iter().any(|d| d["label"] == "gpt-test"));
        assert!(report["icon"].as_str().unwrap().starts_with("<svg"));
        let details = report["report"]["details"].as_array().unwrap();
        let find = |id: &str| details.iter().find(|d| d["id"] == id).map(|d| d["value"].as_str().unwrap().to_string());
        // One day tracked: the projection is that day times 30.
        assert_eq!(find("projected-month").unwrap(), usd(d.cost * 30.0));
        assert_eq!(find("cache-hit").unwrap(), "40%");
        assert!(find(format!("day-{}", day(0)).as_str()).is_some());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn litellm_list_is_filtered_and_matched() {
        let body = r#"{
            "sample_spec": { "litellm_provider": "one of the providers", "input_cost_per_token": 0.0 },
            "claude-opus-9": { "litellm_provider": "anthropic", "input_cost_per_token": 5e-6, "output_cost_per_token": 2.5e-5, "cache_read_input_token_cost": 5e-7 },
            "bedrock/claude-opus-9": { "litellm_provider": "bedrock", "input_cost_per_token": 1, "output_cost_per_token": 1 },
            "gemini-x": { "litellm_provider": "gemini", "input_cost_per_token": 1, "output_cost_per_token": 1 }
        }"#;
        let prices = parse_litellm(body).unwrap();
        assert_eq!(prices.len(), 1);
        let p = prices["claude-opus-9"];
        assert_eq!(p.cache_write, 5e-6 * 1.25);

        assert!(lookup(&prices, "claude-opus-9[1m]").is_some());
        assert!(lookup(&prices, "claude-opus-9-20260101").is_some());
        assert!(lookup(&prices, "anthropic/claude-opus-9").is_some());
        assert!(lookup(&prices, "claude-opus-8").is_none());
    }

    #[test]
    fn formatting() {
        assert_eq!(short(999), "999");
        assert_eq!(short(1_250), "1.2K");
        assert_eq!(short(3_400_000), "3.4M");
        assert_eq!(usd(0.004), "$0.0040");
        assert_eq!(usd(12.345), "$12.35");
    }
}
