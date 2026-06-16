import os
from dotenv import load_dotenv
load_dotenv()

print("HOST:", os.environ.get("DATABRICKS_HOST", "NOT SET"))
print("TOKEN:", "SET" if os.environ.get("DATABRICKS_TOKEN") else "NOT SET")
print("WAREHOUSE:", os.environ.get("SQL_WAREHOUSE_ID", "NOT SET"))

import dash
from dash import Dash, html, dcc, Input, Output, State, callback_context, no_update
import dash_bootstrap_components as dbc

from app.layout import build_layout
from app.genie_client import GenieClient
from app.alerts import AlertEngine

try:
    from app.data import (
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
ALERT_EMAIL    = os.environ.get("ALERT_EMAIL", "gresahasani19@gmail.com")

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
    print(f"Genie disabled locally: {e}")
    genie = None

alerts_engine = AlertEngine()

app.layout = build_layout()


# ── Nav routing
@app.callback(
    Output("active-view",    "data"),
    Output("crumb",          "children"),
    Output("nav-overview",   "className"),
    Output("nav-pipeline",   "className"),
    Output("nav-assets",     "className"),
    Output("nav-gaps",       "className"),
    Output("nav-genie",      "className"),
    Output("nav-alerts",     "className"),
    Input("nav-overview",    "n_clicks"),
    Input("nav-pipeline",    "n_clicks"),
    Input("nav-assets",      "n_clicks"),
    Input("nav-gaps",        "n_clicks"),
    Input("nav-genie",       "n_clicks"),
    Input("nav-alerts",      "n_clicks"),
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
        "nav-alerts-top": ("alerts",     "Alert rules"),
    }
    view, label = view_map.get(btn_id, ("overview", "Overview"))

    nav_ids = ["nav-overview", "nav-pipeline", "nav-assets",
               "nav-gaps", "nav-genie", "nav-alerts"]
    classes = []
    for nav_id in nav_ids:
        view_key = nav_id.replace("nav-", "")
        classes.append("nb on" if view_key == view else "nb")

    return view, label, *classes


@app.callback(
    Output("active-view",   "data",      allow_duplicate=True),
    Output("crumb",         "children",  allow_duplicate=True),
    Output("nav-overview",  "className", allow_duplicate=True),
    Output("nav-pipeline",  "className", allow_duplicate=True),
    Output("nav-assets",    "className", allow_duplicate=True),
    Output("nav-gaps",      "className", allow_duplicate=True),
    Output("nav-genie",     "className", allow_duplicate=True),
    Output("nav-alerts",    "className", allow_duplicate=True),
    Input("ask-genie-btn",  "n_clicks"),
    prevent_initial_call=True,
)
def go_to_genie(_):
    return "genie", "Genie", "nb", "nb", "nb", "nb", "nb on", "nb"

@app.callback(
    Output("alerts-container", "children"),
    Input("db-alerts-store", "data"),
    Input("refresh-interval", "n_intervals"),
)
def update_alerts_view(_, __):
    from app.data import load_alerts
    alerts = load_alerts()

    if not alerts:
        return html.Div("No alerts found.",
                        style={"fontSize":"12px","color":"#888","padding":"12px"})

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
                html.Div(f"Owner: {alert.get('owner','—')} · Every 1 hour", className="ar-meta"),
            ], className="ar-body"),
            html.Span(label, className=f"pill {cls}"),
        ], className="alert-row",
           style={"display":"flex","alignItems":"center","gap":"10px"}))

    return items


# ── Refresh all Gold data
@app.callback(
    Output("kpi-store",      "data"),
    Output("pipeline-store", "data"),
    Output("gaps-store",     "data"),
    Output("assets-store",   "data"),
    Output("domain-store",   "data"),
    Output("db-alerts-store", "data"),
    Output("run-pill",       "children"),
    Input("refresh-interval",   "n_intervals"),
    Input("manual-refresh-btn", "n_clicks"),
)

def refresh_all_data(_intervals, _clicks):
    if not DATA_AVAILABLE:
        return {}, {}, [], [], [], [html.Span(className="rdot warn"), " No DB connection"], []

    try:
        kpis      = load_kpi_summary()
        pq        = load_pipeline_quality()
        ss        = load_structural_summary()
        gaps      = load_gaps()
        assets    = load_table_governance()
        domains   = load_domain_summary()
        pl        = load_pipeline_status()
        db_alerts = load_alerts()

        alerts_engine.evaluate_and_fire(kpis=kpis, pq=pq, ss=ss, gaps=gaps)

        warnings = pl.get("alerts_fired", 0)
        dot_cls  = "rdot warn" if warnings > 0 else "rdot ok"
        pill = [html.Span(className=dot_cls),
                f" Run complete · {warnings} warning{'s' if warnings != 1 else ''}"]

        return kpis, pl, gaps, assets, domains, pill, db_alerts

    except Exception as e:
        print(f"Data refresh error: {e}")
        return {}, {}, [], [], [], [html.Span(className="rdot warn"), f" Error: {str(e)[:40]}"], []


# ── Render active view
@app.callback(
    Output("main-content", "children"),
    Input("active-view",    "data"),
    Input("kpi-store",      "data"),
    Input("pipeline-store", "data"),
    Input("gaps-store",     "data"),
    Input("assets-store",   "data"),
    Input("domain-store",   "data"),
)
def render_view(view, kpis, pipeline, gaps, assets, domains):
    kpis     = kpis     or {}
    pipeline = pipeline or {}
    gaps     = gaps     or []
    assets   = assets   or []
    domains  = domains  or []

    if view == "overview":  return _view_overview(kpis, pipeline, domains)
    if view == "pipeline":  return _view_pipeline(pipeline)
    if view == "assets":    return _view_assets(assets)
    if view == "gaps":      return _view_gaps(gaps)
    if view == "genie":     return _view_genie()
    if view == "alerts":    return _view_alerts()
    return _view_overview(kpis, pipeline, domains)


# ── View builders

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


def _view_overview(kpis, pipeline, domains):
    return html.Div([
        html.Div([
            html.Div([
                html.Div("Overview", className="vt"),
            ]),
            html.Div(
                html.Button([html.I(className="ti ti-message-chatbot"), " Ask Genie"],
                            id="ask-genie-btn", className="btn p", n_clicks=0),
                className="ha"
            ),
        ], className="vh"),

        html.Iframe(
            src="https://dbc-3a1cd165-94e4.cloud.databricks.com/embed/dashboardsv3/01f168ff1cb21c6ebaa55ea7c5b43210",
            style={
                "width": "100%",
                "height": "calc(100vh - 120px)",
                "border": "none",
                "borderRadius": "8px",
            }
        ),
    ], className="view")


def _view_pipeline(pipeline):
    steps   = pipeline.get("steps", [])
    qrows   = pipeline.get("quarantine_rows", 0)
    afired  = pipeline.get("alerts_fired", 0)
    total   = pipeline.get("total_rows", 0)

    sm = {"ok": ("si ok", "ti ti-check"), "warn": ("si wn", "ti ti-alert-triangle"), "fail": ("si fl", "ti ti-x")}
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
        html.Div([html.I(className="ti ti-database"), " Live · gold.dlt_summary + gold.structural_summary"], className="sim-badge"),
        html.Div([
            html.Div([html.Div("Pipeline status", className="vt"),
                      html.Div("Bronze → Silver → Gold", className="vm")]),
        ], className="vh"),
        html.Div([
            html.Div([html.Span("Current run", className="ct"),
                      _pill(f"{afired} warning(s)", "p-wn") if afired > 0 else _pill("All clear", "p-ok")],
                     className="ch"),
            html.Div(step_rows, className="pr"),
        ], className="card"),
        html.Div([
            _mk("Total rows",      f"{total:,}", None),
            _mk("Quarantined",     str(qrows),   "above threshold" if qrows > 0 else "within threshold",
                sub_cls="mk-s dn" if qrows > 0 else "mk-s"),
            _mk("Alerts fired",    str(afired),  "check overview" if afired > 0 else "none"),
        ], className="mg3"),
    ], className="view")


def _view_assets(assets):
    rows = []
    for a in assets[:200]:
        score = float(a.get("governance_score") or 0)
        rows.append(html.Tr([
            html.Td(a.get("table_name", ""), style={"fontWeight": "500"}),
            html.Td(html.Span(a.get("schema_name", ""), className="pill p-nu")),
            html.Td(a.get("database_name", "—"), style={"color": "#888"}),
            html.Td(a.get("system_name", "—"), style={"color": "#888"}),
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
                                    style={"textAlign": "center", "padding": "20px",
                                           "color": "#888"}))
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
        reason = g.get("gap_reason", "")
        sc, sl, pc = sev(reason)
        reasons = [r.strip() for r in reason.split(",") if r.strip()]
        items.append(html.Div([
            html.Div(className=f"gs {sc}", style={"minHeight": "40px"}),
            html.Div([
                html.Div(
                    f"{g.get('table_name','?')} — {g.get('column_name','?')}",
                    className="gt"
                ),
                html.Div(
                    f"{g.get('schema_name','')} · {g.get('database_name','')} · "
                    f"completeness {float(g.get('completeness_pct') or 0):.0f}%",
                    className="gm"
                ),
                html.Div([
                    html.Div(
                        f"• {r}",
                        style={"fontSize":"11px","color":"#534AB7","marginBottom":"2px"}
                    )
                    for r in reasons
                ], style={"marginTop":"4px"}),
            ]),
            _pill(sl, pc),
        ], className="gap-item"))

    return html.Div([
        html.Div(
            [html.I(className="ti ti-database"), " Live · gold.governance_gaps"],
            className="sim-badge"
        ),
        html.Div([
            html.Div([
                html.Div("Governance gaps", className="vt"),
                html.Div(f"{len(gaps)} open issues", className="vm"),
            ]),
        ], className="vh"),
        html.Div(
            items if items else [
                html.Div(
                    "No governance gaps — all checks passing.",
                    style={"padding":"12px","fontSize":"12px","color":"#888"}
                )
            ],
            className="card"
        ),
    ], className="view")


def _view_genie():
    return html.Div([
        html.Div([html.Div([html.Div("Genie", className="vt"),
                             html.Div("Metadata Governance Assistant · Gold-grounded", className="vm")]),
                  html.Div(html.Button([html.I(className="ti ti-trash"), " Clear"],
                                        id="clear-chat-btn", className="btn", n_clicks=0), className="ha")],
                 className="vh"),
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
                  " Live · Databricks SQL Alerts API · updates automatically"],
                 className="sim-badge"),
        html.Div([
            html.Div([html.Div("Alert rules", className="vt"),
                      html.Div("All workspace alerts · synced automatically", className="vm")]),
        ], className="vh"),
        html.Div(
            id="alerts-container",
            children=[html.Div("Loading...",
                               style={"fontSize":"12px","color":"#888","padding":"12px"})],
            className="card"
        ),
    ], className="view")


# ── Genie chat callback
@app.callback(
    Output("genie-msgs",  "children"),
    Output("genie-input", "value"),
    State("genie-msgs",   "children"),
    State("genie-input",  "value"),
    Input("genie-send-btn",  "n_clicks"),
    Input("clear-chat-btn",  "n_clicks"),
    prevent_initial_call=True,
)
def handle_genie(current_msgs, question, send_clicks, clear_clicks):
    ctx  = callback_context
    if not ctx.triggered:
        return no_update, no_update
    trig = ctx.triggered[0]["prop_id"].split(".")[0]

    if trig == "clear-chat-btn":
        if genie:
            genie.reset()
        return [html.Div([
            html.Div(html.I(className="ti ti-robot"), className="mav g"),
            html.Div(html.Div("Chat cleared. Ready for a new conversation.", className="bub")),
        ], className="msg g")], ""

    if not question or not question.strip():
        return no_update, no_update

    messages = list(current_msgs or [])
    messages.append(html.Div([
        html.Div("GH", className="mav u2"),
        html.Div(html.Div(question, className="bub")),
    ], className="msg u"))

    try:
        if genie is None:
            raise RuntimeError("Genie not available locally — will work in Databricks App.")
        answer = genie.ask(question)
    except Exception as e:
        answer = str(e)

    messages.append(html.Div([
        html.Div(html.I(className="ti ti-robot"), className="mav g"),
        html.Div([
            dcc.Markdown(
                answer,
                dangerously_allow_html=False,
                style={
                    "background": "#EFEFED",
                    "border": "0.5px solid rgba(0,0,0,0.1)",
                    "borderRadius": "3px 10px 10px 10px",
                    "padding": "10px 14px",
                    "fontSize": "12px",
                    "lineHeight": "1.7",
                }
            ),
            html.Div([
                html.Span("Is this useful?", style={"fontSize":"11px","color":"#888"}),
                html.Button("👍", n_clicks=0, style={"marginLeft":"8px","background":"none","border":"none","cursor":"pointer"}),
                html.Button("👎", n_clicks=0, style={"marginLeft":"4px","background":"none","border":"none","cursor":"pointer"}),
            ], style={"display":"flex","alignItems":"center","marginTop":"6px"}),
        ]),
    ], className="msg g"))

    return messages, ""

if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 8050)),
        debug=False,
    )
