import os
import time
import datetime
from dotenv import load_dotenv
load_dotenv()

print("=== ENV CHECK ===")
print(f"HOST:          {os.environ.get('DATABRICKS_HOST',      'NOT SET')}")
print(f"CLIENT_ID:     {os.environ.get('DATABRICKS_CLIENT_ID', 'NOT SET')}")
print(f"CLIENT_SECRET: {'SET' if os.environ.get('DATABRICKS_CLIENT_SECRET') else 'NOT SET'}")
print(f"GENIE_SPACE:   {os.environ.get('GENIE_SPACE_ID',       'NOT SET')}")
print(f"WAREHOUSE:     {os.environ.get('SQL_WAREHOUSE_ID',     'NOT SET')}")
print("=================")

import dash
from dash import Dash, html, dcc, Input, Output, State, callback_context, no_update, MATCH
import dash_bootstrap_components as dbc

from app.layout import build_layout
from app.genie_client import GenieClient
from app.alerts import AlertEngine

try:
    from app.data import (
        _connect,
        load_kpi_summary,
        load_pipeline_quality,
        load_structural_summary,
        load_gaps,
        load_table_governance,
        load_domain_summary,
        load_pipeline_status,
        load_alerts,
    )
    DATA_AVAILABLE = True
except Exception as e:
    print(f"Data layer error: {e}")
    DATA_AVAILABLE = False

GENIE_SPACE_ID = os.environ.get("GENIE_SPACE_ID", "")
GENIE_VERSION  = "2.0"

app = Dash(
    __name__,
    external_stylesheets=[
        "https://cdn.jsdelivr.net/npm/@tabler/icons-webfont@2.44.0/tabler-icons.min.css",
    ],
    suppress_callback_exceptions=True,
    title="Metadata Governance Platform",
)
server = app.server

try:
    genie = GenieClient(space_id=GENIE_SPACE_ID)
except Exception as e:
    print(f"Genie disabled: {e}")
    genie = None

alerts_engine = AlertEngine()
app.layout = build_layout()


# ── Nav routing
@app.callback(
    Output("active-view",      "data"),
    Output("crumb",            "children"),
    Output("nav-overview",     "className"),
    Output("nav-pipeline",     "className"),
    Output("nav-assets",       "className"),
    Output("nav-gaps",         "className"),
    Output("nav-genie",        "className"),
    Output("nav-alerts",       "className"),
    Output("nav-monitoring",   "className"),
    Input("nav-overview",      "n_clicks"),
    Input("nav-pipeline",      "n_clicks"),
    Input("nav-assets",        "n_clicks"),
    Input("nav-gaps",          "n_clicks"),
    Input("nav-genie",         "n_clicks"),
    Input("nav-alerts",        "n_clicks"),
    Input("nav-monitoring",    "n_clicks"),
    prevent_initial_call=True,
)
def route_nav(*_):
    ctx = callback_context
    if not ctx.triggered:
        return no_update, no_update, "nb on", "nb", "nb", "nb", "nb", "nb", "nb"

    btn_id = ctx.triggered[0]["prop_id"].split(".")[0]
    view_map = {
        "nav-overview":   ("overview",   "Overview"),
        "nav-pipeline":   ("pipeline",   "Pipeline status"),
        "nav-assets":     ("assets",     "Asset explorer"),
        "nav-gaps":       ("gaps",       "Governance gaps"),
        "nav-genie":      ("genie",      "Genie"),
        "nav-alerts":     ("alerts",     "Alert rules"),
        "nav-monitoring": ("monitoring", "Genie Monitoring"),
    }
    view, label = view_map.get(btn_id, ("overview", "Overview"))

    nav_ids = ["nav-overview", "nav-pipeline", "nav-assets",
               "nav-gaps", "nav-genie", "nav-alerts", "nav-monitoring"]
    classes = ["nb on" if nid.replace("nav-", "") == view else "nb" for nid in nav_ids]

    return view, label, *classes


# ── Ask Genie button
@app.callback(
    Output("active-view",    "data",      allow_duplicate=True),
    Output("crumb",          "children",  allow_duplicate=True),
    Output("nav-overview",   "className", allow_duplicate=True),
    Output("nav-pipeline",   "className", allow_duplicate=True),
    Output("nav-assets",     "className", allow_duplicate=True),
    Output("nav-gaps",       "className", allow_duplicate=True),
    Output("nav-genie",      "className", allow_duplicate=True),
    Output("nav-alerts",     "className", allow_duplicate=True),
    Output("nav-monitoring", "className", allow_duplicate=True),
    Input("ask-genie-btn",   "n_clicks"),
    prevent_initial_call=True,
)
def go_to_genie(_):
    return "genie", "Genie", "nb", "nb", "nb", "nb", "nb on", "nb", "nb"


# ── Refresh all Gold data
@app.callback(
    Output("kpi-store",       "data"),
    Output("pipeline-store",  "data"),
    Output("gaps-store",      "data"),
    Output("assets-store",    "data"),
    Output("domain-store",    "data"),
    Output("db-alerts-store", "data"),
    Output("run-pill",        "children"),
    Input("refresh-interval",   "n_intervals"),
    Input("manual-refresh-btn", "n_clicks"),
)
def refresh_all_data(_intervals, _clicks):
    if not DATA_AVAILABLE:
        return {}, {}, [], [], [], [], [html.Span(className="rdot warn"), " No DB connection"]

    try:
        # Single connection for all queries — avoids 10 separate TCP handshakes
        with _connect() as conn:
            kpis      = load_kpi_summary(conn)
            pq        = load_pipeline_quality(conn)
            ss        = load_structural_summary(conn)
            gaps      = load_gaps(conn)
            assets    = load_table_governance(conn=conn)
            domains   = load_domain_summary(conn)
            db_alerts = load_alerts(conn)
            pl        = load_pipeline_status(pq, ss)  # reuses already-fetched pq/ss

        alerts_engine.evaluate_and_fire(kpis=kpis, pq=pq, ss=ss, gaps=gaps)

        warnings = pl.get("alerts_fired", 0)
        dot_cls  = "rdot warn" if warnings > 0 else "rdot ok"
        pill = [html.Span(className=dot_cls),
                f" Run complete · {warnings} warning{'s' if warnings != 1 else ''}"]

        return kpis, pl, gaps, assets, domains, db_alerts, pill

    except Exception as e:
        print(f"Data refresh error: {e}")
        return {}, {}, [], [], [], [], [html.Span(className="rdot warn"), f" Error: {str(e)[:40]}"]


# ── Alerts view
@app.callback(
    Output("alerts-container", "children"),
    Input("db-alerts-store",   "data"),
)
def update_alerts_view(alerts):
    alerts = alerts or []

    if not alerts:
        return html.Div("No alerts found.",
                        style={"fontSize": "12px", "color": "#888", "padding": "12px"})

    status_map = {
        "ok":        ("p-ok", "OK",        "ti-check"),
        "triggered": ("p-cr", "TRIGGERED", "ti-alert-circle"),
        "unknown":   ("p-nu", "UNKNOWN",   "ti-question-mark"),
    }

    items = []
    for alert in alerts:
        status = (alert.get("status") or "unknown").lower()
        cls, label, icon = status_map.get(status, ("p-nu", "UNKNOWN", "ti-question-mark"))
        items.append(html.Div([
            html.Div(
                html.I(className=f"ti {icon}",
                       style={"fontSize": "16px",
                              "color": "#1D9E75" if status == "ok"
                                       else "#E24B4A" if status == "triggered"
                                       else "#888"}),
                style={"flexShrink": "0"}
            ),
            html.Div([
                html.Div(alert.get("name", ""), className="ar-title"),
                html.Div(f"Owner: {alert.get('owner', '—')} · Every 1 hour", className="ar-meta"),
            ], className="ar-body"),
            html.Span(label, className=f"pill {cls}"),
        ], className="alert-row",
           style={"display": "flex", "alignItems": "center", "gap": "10px"}))

    return items


# ── Render active view
# monitoring-store is State (not Input) to prevent re-render wiping the chat
@app.callback(
    Output("main-content", "children"),
    Input("active-view",      "data"),
    Input("kpi-store",        "data"),
    Input("pipeline-store",   "data"),
    Input("gaps-store",       "data"),
    Input("assets-store",     "data"),
    Input("domain-store",     "data"),
    State("monitoring-store", "data"),
)
def render_view(view, kpis, pipeline, gaps, assets, domains, mon_data):
    kpis     = kpis     or {}
    pipeline = pipeline or {}
    gaps     = gaps     or []
    assets   = assets   or []
    domains  = domains  or []
    mon_data = mon_data or []

    if view == "overview":   return _view_overview(kpis, pipeline, domains)
    if view == "pipeline":   return _view_pipeline(pipeline)
    if view == "assets":     return _view_assets(assets)
    if view == "gaps":       return _view_gaps(gaps)
    if view == "genie":      return _view_genie()
    if view == "alerts":     return _view_alerts()
    if view == "monitoring": return _view_monitoring(mon_data)
    return _view_overview(kpis, pipeline, domains)


# ── View helpers
def _mk(label, value, sub=None, sub_cls="mk-s"):
    children = [html.Div(label, className="mk-l"), html.Div(str(value), className="mk-v")]
    if sub:
        children.append(html.Div(sub, className=sub_cls))
    return html.Div(children, className="mk")

def _bar(label, pct, color):
    return html.Div([
        html.Span(label, className="bl"),
        html.Div(html.Div(style={"width": f"{float(pct):.1f}%", "height": "4px",
                                  "background": color, "borderRadius": "99px"}), className="bt"),
        html.Span(f"{float(pct):.1f}%", className="bv"),
    ], className="brow")

def _pill(text, cls):
    return html.Span(text, className=f"pill {cls}")

def _cert_color(v):
    v = float(v or 0)
    return "#639922" if v >= 80 else "#378ADD" if v >= 60 else "#BA7517" if v >= 40 else "#E24B4A"


# ── Views
def _view_overview(kpis, pipeline, domains):
    return html.Div([
        html.Div([
            html.Div([html.Div("Overview", className="vt")]),
            html.Div(
                html.Button([html.I(className="ti ti-message-chatbot"), " Ask Genie"],
                            id="ask-genie-btn", className="btn p", n_clicks=0),
                className="ha"
            ),
        ], className="vh"),

        html.Div([
            _mk("Databases",    int(kpis.get("total_databases", 0))),
            _mk("Schemas",      int(kpis.get("total_schemas",   0))),
            _mk("Tables",       int(kpis.get("total_tables",    0))),
            _mk("Columns",      int(kpis.get("total_columns",   0))),
            _mk("DQ Pass",      f"{kpis.get('dq_pass_pct', 0)}%"),
            _mk("Completeness", f"{kpis.get('avg_completeness_pct', 0)}%"),
            _mk("PII Columns",  int(kpis.get("pii_columns",     0))),
            _mk("Certified",    int(kpis.get("certified_columns", 0))),
        ], className="mg"),

        html.Div([
            html.Div([html.Span("Domain governance scores", className="ct")], className="ch"),
            html.Div([
                _bar(d.get("domain", ""), d.get("avg_governance_score", 0), "#534AB7")
                for d in (domains or [])[:8]
            ]),
        ], className="card"),

        html.Div([
            html.Div([html.Span("Certification distribution", className="ct")], className="ch"),
            html.Div([
                _bar("Certified",    kpis.get("certified_columns",    0) / max(kpis.get("total_columns", 1), 1) * 100, "#1D9E75"),
                _bar("Documented",   kpis.get("documented_columns",   0) / max(kpis.get("total_columns", 1), 1) * 100, "#378ADD"),
                _bar("Registered",   kpis.get("registered_columns",   0) / max(kpis.get("total_columns", 1), 1) * 100, "#BA7517"),
                _bar("Unclassified", kpis.get("unclassified_columns", 0) / max(kpis.get("total_columns", 1), 1) * 100, "#E24B4A"),
            ]),
        ], className="card"),

    ], className="view")


def _view_pipeline(pipeline):
    steps  = pipeline.get("steps", [])
    qrows  = pipeline.get("quarantine_rows", 0)
    afired = pipeline.get("alerts_fired", 0)
    total  = pipeline.get("total_rows", 0)

    sm = {"ok":   ("si ok", "ti ti-check"),
          "warn": ("si wn", "ti ti-alert-triangle"),
          "fail": ("si fl", "ti ti-x")}
    bm = {"ok": ("pb-ok", "Pass"), "warn": ("pb-wn", "Warning"), "fail": ("pb-fl", "Failed")}

    step_rows = []
    for s in steps:
        st = s.get("status", "ok")
        si_cls, ic = sm.get(st, ("si ok", "ti ti-check"))
        bc, bl = bm.get(st, ("pb-ok", "Pass"))
        step_rows.append(html.Div([
            html.Div(html.I(className=ic), className=si_cls),
            html.Div([html.Div(s.get("name", ""), className="pn"),
                      html.Div(s.get("detail", ""), className="pd")]),
            html.Span("", className="pdur"),
            html.Span(bl, className=f"pbg {bc}"),
        ], className="pr-row"))

    return html.Div([
        html.Div([html.I(className="ti ti-database"),
                  " Live · gold.dlt_summary + gold.structural_summary"], className="sim-badge"),
        html.Div([html.Div([html.Div("Pipeline status", className="vt"),
                             html.Div("Bronze → Silver → Gold", className="vm")])], className="vh"),
        html.Div([
            html.Div([html.Span("Current run", className="ct"),
                      _pill(f"{afired} warning(s)", "p-wn") if afired > 0 else _pill("All clear", "p-ok")],
                     className="ch"),
            html.Div(step_rows, className="pr"),
        ], className="card"),
        html.Div([
            _mk("Total rows",   f"{total:,}"),
            _mk("Quarantined",  str(qrows), "above threshold" if qrows > 0 else "within threshold",
                sub_cls="mk-s dn" if qrows > 0 else "mk-s"),
            _mk("Alerts fired", str(afired), "check overview" if afired > 0 else "none"),
        ], className="mg3"),
    ], className="view")


def _view_assets(assets):
    rows = []
    for a in assets[:200]:
        score = float(a.get("governance_score") or 0)
        rows.append(html.Tr([
            html.Td(a.get("table_name", ""),      style={"fontWeight": "500"}),
            html.Td(html.Span(a.get("schema_name", ""), className="pill p-nu")),
            html.Td(a.get("database_name", "—"),  style={"color": "#888"}),
            html.Td(a.get("system_name", "—"),    style={"color": "#888"}),
            html.Td(a.get("data_steward") or "—", style={"color": "#888"}),
            html.Td(f"{score:.1f}"),
            html.Td(str(a.get("column_count", ""))),
            html.Td(str(a.get("pii_column_count", ""))),
        ]))

    return html.Div([
        html.Div([html.I(className="ti ti-database"),
                  " Live · gold.table_governance · ordered by governance score"],
                 className="sim-badge"),
        html.Div([html.Div([html.Div("Asset explorer", className="vt"),
                             html.Div(f"{len(assets)} tables · live from Gold", className="vm")])],
                 className="vh"),
        html.Div([
            html.Div(html.Table([
                html.Thead(html.Tr([
                    html.Th("Table"), html.Th("Schema"), html.Th("Database"),
                    html.Th("System"), html.Th("Steward"),
                    html.Th("Gov score"), html.Th("Columns"), html.Th("PII cols"),
                ])),
                html.Tbody(rows if rows else [
                    html.Tr(html.Td("No data.", colSpan=8,
                                    style={"textAlign": "center", "padding": "20px", "color": "#888"}))
                ]),
            ], className="dt"), className="tb-wrap"),
        ], className="card", style={"padding": "0", "overflow": "hidden"}),
    ], className="view")


def _view_gaps(gaps):
    sev_map = {
        "steward":        ("cr", "Critical", "p-cr"),
        "pii":            ("cr", "Critical", "p-cr"),
        "sensitivity":    ("cr", "Critical", "p-cr"),
        "quality":        ("hi", "High",     "p-wn"),
        "security":       ("hi", "High",     "p-wn"),
        "classification": ("hi", "High",     "p-wn"),
        "domain":         ("md", "Medium",   "p-md"),
        "description":    ("md", "Medium",   "p-md"),
        "term":           ("md", "Medium",   "p-md"),
    }

    def sev(reason):
        r = (reason or "").lower()
        for k, v in sev_map.items():
            if k in r:
                return v
        return ("md", "Medium", "p-md")

    items = []
    for g in gaps[:60]:
        reason     = g.get("gap_reason", "")
        sc, sl, pc = sev(reason)
        reasons    = [r.strip() for r in reason.split(",") if r.strip()]
        items.append(html.Div([
            html.Div(className=f"gs {sc}", style={"minHeight": "40px"}),
            html.Div([
                html.Div(f"{g.get('table_name','?')} — {g.get('column_name','?')}", className="gt"),
                html.Div(
                    f"{g.get('schema_name','')} · {g.get('database_name','')} · "
                    f"completeness {float(g.get('completeness_pct') or 0):.0f}%",
                    className="gm"
                ),
                html.Div([
                    html.Div(f"• {r}",
                             style={"fontSize": "11px", "color": "#534AB7", "marginBottom": "2px"})
                    for r in reasons
                ], style={"marginTop": "4px"}),
            ]),
            _pill(sl, pc),
        ], className="gap-item"))

    return html.Div([
        html.Div([html.I(className="ti ti-database"), " Live · gold.governance_gaps"],
                 className="sim-badge"),
        html.Div([html.Div([html.Div("Governance gaps", className="vt"),
                             html.Div(f"{len(gaps)} open issues", className="vm")])], className="vh"),
        html.Div(items if items else [
            html.Div("No governance gaps — all checks passing.",
                     style={"padding": "12px", "fontSize": "12px", "color": "#888"})
        ], className="card"),
    ], className="view")


def _view_genie():
    return html.Div([
        html.Div([
            html.Div([html.Div("Genie", className="vt"),
                      html.Div("Metadata Governance Assistant · Gold-grounded", className="vm")]),
            html.Div(html.Button([html.I(className="ti ti-trash"), " Clear"],
                                  id="clear-chat-btn", className="btn", n_clicks=0), className="ha"),
        ], className="vh"),
        html.Div([
            html.Div(html.I(className="ti ti-robot"), className="gav"),
            html.Div([html.Div("Metadata Governance Assistant", className="gn"),
                      html.Div("Grounded on Gold tables · no live compute", className="gs2")]),
            html.Div([html.Span(className="cdot"), " Connected"], className="conn"),
        ], className="chat-hd"),
        html.Div([
            html.Div([
                html.Div(html.I(className="ti ti-robot"), className="mav g"),
                html.Div(html.Div(
                    "Governance Assistant ready. Ask about certification levels, "
                    "DQ issues, stewardship, governance gaps, or access eligibility.",
                    className="bub"
                )),
            ], className="msg g"),
        ], className="msgs", id="genie-msgs"),
        html.Div([
            dcc.Textarea(id="genie-input", placeholder="Ask about any asset, check, or gap...",
                         className="ci", style={"minHeight": "34px", "maxHeight": "72px"}),
            html.Button(html.I(className="ti ti-arrow-up"), id="genie-send-btn",
                        className="sb2", n_clicks=0),
        ], className="ci-row"),
    ], className="view")


def _view_alerts():
    return html.Div([
        html.Div([html.I(className="ti ti-bell"),
                  " Live · Databricks SQL Alerts · updates automatically"],
                 className="sim-badge"),
        html.Div([html.Div([html.Div("Alert rules", className="vt"),
                             html.Div("All workspace alerts · synced automatically", className="vm")])],
                 className="vh"),
        html.Div(
            id="alerts-container",
            children=[html.Div("Loading...",
                               style={"fontSize": "12px", "color": "#888", "padding": "12px"})],
            className="card"
        ),
    ], className="view")


def _view_monitoring(mon_data):
    mon_data     = mon_data or []
    total        = len(mon_data)
    helpful      = sum(1 for e in mon_data if e.get("feedback") == "helpful")
    not_help     = sum(1 for e in mon_data if e.get("feedback") == "not_helpful")
    pending      = total - helpful - not_help
    avg_time     = round(sum(e.get("response_time_s", 0) for e in mon_data) / max(total, 1), 1)
    helpful_pct  = round(helpful  / max(total, 1) * 100)
    not_help_pct = round(not_help / max(total, 1) * 100)

    entries = []
    for e in reversed(mon_data):
        fb = e.get("feedback")
        fb_pill = html.Span("👍 Helpful",    className="pill p-ok") if fb == "helpful" \
             else html.Span("👎 Not helpful", className="pill p-cr") if fb == "not_helpful" \
             else html.Span("Pending",        className="pill p-nu")
        entries.append(html.Div([
            html.Div([
                html.Div(e.get("question", ""), className="ar-title"),
                html.Div(
                    f"v{e.get('version','?')} · {e.get('ts','')} · "
                    f"{e.get('response_time_s', 0)}s response time",
                    className="ar-meta"
                ),
            ], className="ar-body"),
            fb_pill,
        ], className="alert-row",
           style={"display": "flex", "alignItems": "center", "gap": "10px"}))

    return html.Div([
        html.Div([html.I(className="ti ti-activity"),
                  f" Genie Monitoring · session · instructions v{GENIE_VERSION}"],
                 className="sim-badge"),
        html.Div([html.Div([
            html.Div("Genie Monitoring", className="vt"),
            html.Div(f"Session stats · instructions version {GENIE_VERSION}", className="vm"),
        ])], className="vh"),
        html.Div([
            _mk("Total queries",     str(total),         "this session"),
            _mk("Helpful",           f"{helpful_pct}%",  f"{helpful} responses",  sub_cls="mk-s up"),
            _mk("Not helpful",       f"{not_help_pct}%", f"{not_help} flagged",   sub_cls="mk-s dn" if not_help > 0 else "mk-s"),
            _mk("Avg response time", f"{avg_time}s",     f"{pending} pending feedback"),
        ], className="mg"),
        html.Div([
            html.Div([html.Span("Prompt version", className="ct"),
                      html.Span(f"v{GENIE_VERSION}", className="pill p-doc",
                                style={"marginLeft": "8px"})], className="ch"),
            html.Div([
                html.Div("v2.0 — Current",
                         style={"fontSize": "12px", "fontWeight": "500", "color": "#1a1a2e"}),
                html.Div(
                    "Trusted datasets: gold.profile · gold.table_governance · "
                    "gold.governance_gaps · gold.kpi_summary · gold.dlt_summary · "
                    "gold.user_profile · gold.asset_access_policy · gold.asset_access_check",
                    style={"fontSize": "11px", "color": "#534AB7", "marginTop": "4px"}
                ),
            ], style={"padding": "4px 0"}),
        ], className="card"),
        html.Div([
            html.Div([html.Span("Query log", className="ct"),
                      html.Span(f"{total} entries", className="cn")], className="ch"),
            html.Div(entries if entries else [
                html.Div("No queries yet — ask Genie something first.",
                         style={"fontSize": "12px", "color": "#888", "padding": "10px 0"})
            ]),
        ], className="card"),
    ], className="view")


# ── Genie chat callback
@app.callback(
    Output("genie-msgs",       "children"),
    Output("genie-input",      "value"),
    Output("monitoring-store", "data"),
    State("genie-msgs",        "children"),
    State("genie-input",       "value"),
    State("monitoring-store",  "data"),
    Input("genie-send-btn",    "n_clicks"),
    Input("clear-chat-btn",    "n_clicks"),
    prevent_initial_call=True,
)
def handle_genie(current_msgs, question, mon_data, send_clicks, clear_clicks):
    import traceback

    ctx  = callback_context
    trig = ctx.triggered[0]["prop_id"].split(".")[0] if ctx.triggered else None

    print(f"handle_genie: trig={trig} question='{question}'")

    mon_data = list(mon_data or [])

    if trig == "clear-chat-btn":
        if genie:
            genie.reset()
        return [html.Div([
            html.Div(html.I(className="ti ti-robot"), className="mav g"),
            html.Div(html.Div("Chat cleared. Ready for a new conversation.", className="bub")),
        ], className="msg g")], "", mon_data

    if not trig:
        return no_update, no_update, no_update

    if not question or not question.strip():
        return no_update, no_update, no_update

    messages = list(current_msgs or [])
    entry_id = len(mon_data)

    # Add user message immediately
    messages.append(html.Div([
        html.Div("GH", className="mav u2"),
        html.Div(html.Div(question, className="bub")),
    ], className="msg u"))

    print(f"Sending to Genie: '{question}'")

    start_time = time.time()
    try:
        if genie is None:
            raise RuntimeError(
                f"Genie is None — GENIE_SPACE_ID={os.environ.get('GENIE_SPACE_ID', 'NOT SET')}"
            )
        answer = genie.ask(question)
    except Exception as e:
        answer = f"⚠️ Error: {str(e)}\n\n```\n{traceback.format_exc()}\n```"
        print(f"Genie error: {e}")

    response_time = round(time.time() - start_time, 1)
    print(f"Genie answered in {response_time}s: {answer[:100]}")

    mon_data.append({
        "id":              entry_id,
        "question":        question,
        "answer":          answer,
        "feedback":        None,
        "ts":              datetime.datetime.now().strftime("%H:%M"),
        "response_time_s": response_time,
        "version":         GENIE_VERSION,
    })

    messages.append(html.Div([
        html.Div(html.I(className="ti ti-robot"), className="mav g"),
        html.Div([
            dcc.Markdown(
                answer,
                dangerously_allow_html=False,
                style={
                    "background":   "#EFEFED",
                    "border":       "0.5px solid rgba(0,0,0,0.1)",
                    "borderRadius": "3px 10px 10px 10px",
                    "padding":      "10px 14px",
                    "fontSize":     "12px",
                    "lineHeight":   "1.7",
                }
            ),
            html.Div([
                html.Span("Is this useful?", style={"fontSize": "11px", "color": "#888"}),
                html.Button("👍",
                            id={"type": "fb-up", "index": entry_id},
                            n_clicks=0,
                            style={"marginLeft": "8px", "background": "none",
                                   "border": "none", "cursor": "pointer", "fontSize": "14px"}),
                html.Button("👎",
                            id={"type": "fb-dn", "index": entry_id},
                            n_clicks=0,
                            style={"marginLeft": "4px", "background": "none",
                                   "border": "none", "cursor": "pointer", "fontSize": "14px"}),
                html.Span(id={"type": "fb-saved", "index": entry_id},
                          style={"fontSize": "10px", "color": "#888", "marginLeft": "6px"}),
            ], style={"display": "flex", "alignItems": "center", "marginTop": "6px"}),
        ]),
    ], className="msg g"))

    return messages, "", mon_data


# ── Feedback capture callback
@app.callback(
    Output({"type": "fb-saved", "index": MATCH}, "children"),
    Output("monitoring-store", "data", allow_duplicate=True),
    Input({"type": "fb-up",    "index": MATCH}, "n_clicks"),
    Input({"type": "fb-dn",    "index": MATCH}, "n_clicks"),
    State({"type": "fb-up",    "index": MATCH}, "id"),
    State("monitoring-store",  "data"),
    prevent_initial_call=True,
)
def capture_feedback(up_clicks, dn_clicks, btn_id, mon_data):
    ctx = callback_context
    if not ctx.triggered:
        return no_update, no_update

    trig       = ctx.triggered[0]["prop_id"]
    entry_id   = btn_id["index"]
    mon_data   = list(mon_data or [])
    is_helpful = "fb-up" in trig

    for entry in mon_data:
        if entry["id"] == entry_id:
            entry["feedback"] = "helpful" if is_helpful else "not_helpful"
            break

    label = "· logged ✓" if is_helpful else "· flagged for review"
    return label, mon_data


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 8050)),
        debug=False,
    )
