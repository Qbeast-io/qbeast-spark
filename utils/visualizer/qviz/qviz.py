from dash import Dash, html, dcc
from dash.dependencies import Input, Output
import dash_cytoscape as cyto
import click

from qviz.content_loader import process_table, get_nodes_and_edges

cyto.load_extra_layouts()
LAYOUT_NAME = "dagre"
app = Dash(__name__)


@click.command("qviz")
@click.argument("path")
@click.option("-r", "--revision-id", default=1, help="Target index revision.")
def show_tree(path: str, revision_id: int) -> None:
    """
    Display otree index for a given revision(default to 1).

    example:

    qviz ./docs/test_table/

    qviz <local_table_path> --revision-id=2

    WARNING: The sampling details displayed only consider the cubes from the provided
    index revision, so if there are more than one revision, the results will
    be inaccurate.
    """
    assert (
        revision_id > 0
    ), f"Invalid value for revision_id: {revision_id}, it must be > 0."

    cubes, elements = process_table(path, revision_id)

    # Callback that modifies the drawing elements
    @app.callback(Output("cyto-space", "elements"), Input("fraction", "value"))
    def update_fraction_edges(fraction: float) -> list[dict]:
        if fraction is not None and 0.0 < fraction <= 1.0:
            return get_nodes_and_edges(cubes, fraction)
        return elements

    app.layout = html.Div(
        [
            html.P("OTree Index"),
            html.Div(
                [
                    html.Div(
                        style={"width": "50%", "display": "inline"},
                        children=[
                            "Sampling Fraction:",
                            dcc.Input(
                                id="fraction", type="number", step=0.01, value=0.02
                            ),
                        ],
                    )
                ]
            ),
            cyto.Cytoscape(
                id="cyto-space",
                elements=elements,
                layout={"name": LAYOUT_NAME, "roots": "#root"},
                style={"width": "100%", "height": "1000px"},
                stylesheet=[
                    # Display cube max_weight for reach node
                    {"selector": "nodes", "style": {"label": "data(label)"}},
                    # Highlight sampled nodes and edges
                    {
                        "selector": ".sampled",
                        "style": {"background-color": "blue", "line-color": "blue"},
                    },
                ],
            ),
        ]
    )

    app.run_server(debug=True)
