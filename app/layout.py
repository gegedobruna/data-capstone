from dash import html, dcc
import os


def build_layout():

    return html.Div([
        dcc.Store(id="kpi-store"),
        dcc.Store(id="pipeline-store"),
        dcc.Store(id="gaps-store"),
        dcc.Store(id="assets-store"),
        dcc.Store(id="domain-store"),
        dcc.Store(id="db-alerts-store"),
        dcc.Store(id="monitoring-store", data=[]),
        dcc.Store(id="active-view", data="overview"),
        dcc.Interval(id="refresh-interval", interval=5 * 60 * 1000, n_intervals=0),

        # ── Topbar
        html.Div([
            html.Div(
                html.Div(html.I(className="ti ti-shield-check"), className="brand-mark"),
                className="tb-brand"
            ),
            html.Div(className="tb-div"),
            html.Div([
                html.Span("Metadata Governance Platform", className="seg"),
                html.I(className="ti ti-chevron-right"),
                html.Span(id="crumb", children="Overview", className="seg cur"),
            ], className="tb-path"),
            html.Div([
                html.Div(id="run-pill", children=[
                    html.Span(className="rdot ok"),
                    " Connecting...",
                ], className="run-pill"),
                html.Button(
                    html.I(className="ti ti-refresh"),
                    id="manual-refresh-btn",
                    className="ib",
                    n_clicks=0
                ),
            ], className="tb-right"),
        ], className="topbar"),

        html.Div([
            # ── Sidebar nav
            html.Nav([
                html.Button(
                    html.I(className="ti ti-layout-dashboard"),
                    id="nav-overview", className="nb on", n_clicks=0, title="Overview"
                ),
                html.Button(
                    html.I(className="ti ti-timeline"),
                    id="nav-pipeline", className="nb", n_clicks=0, title="Pipeline"
                ),
                html.Div(className="nb-sep"),
                html.Button(
                    html.I(className="ti ti-table"),
                    id="nav-assets", className="nb", n_clicks=0, title="Assets"
                ),
                html.Button(
                    html.I(className="ti ti-alert-triangle"),
                    id="nav-gaps", className="nb", n_clicks=0, title="Gaps"
                ),
                html.Div(className="nb-sep"),
                html.Button(
                    html.I(className="ti ti-message-chatbot"),
                    id="nav-genie", className="nb", n_clicks=0, title="Genie"
                ),
                html.Button(
                    html.I(className="ti ti-bell"),
                    id="nav-alerts", className="nb", n_clicks=0, title="Alerts"
                ),
                html.Button(
                    html.I(className="ti ti-activity"),
                    id="nav-monitoring", className="nb", n_clicks=0, title="Monitoring"
                ),
                html.Div(className="nb-bot"),
                html.Div("GH", className="user-av", title="Gresa Hasani — MLOps Engineer"),
            ], className="nav"),

            # ── Main content
            html.Div(
                id="main-content",
                className="main",
                children=[
                    html.Div(
                        "Loading...",
                        style={"padding": "20px", "color": "#888", "fontSize": "12px"}
                    )
                ]
            ),
        ], className="app-body"),

    ], className="app-shell")
