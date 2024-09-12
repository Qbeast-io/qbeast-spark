from dash import Dash, html, dcc
from dash.dependencies import Input, Output
import dash_cytoscape as cyto
import click

from qviz.content_loader import process_table, delta_nodes_and_edges
from qviz.drawing_elements import process_add_files, populate_tree, get_nodes_and_edges


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
    assert revision_id > 0, f"Invalid value for revision_id: {revision_id}, it must be > 0."

    # Gather revision AddFiles from the table _delta_log/
    #add_files, metadata = process_table_delta_log(path, str(revision_id))
    # Process build Cubes from AddFiles
    #cubes = process_add_files(add_files, metadata)
    # Populate the index by creating parent-child references
    #_ = populate_tree(cubes)
    # Gather drawing elements from populated cubes
    #elements = get_nodes_and_edges(cubes)

    metadata, cubes, _, elements = process_table(path, revision_id)

    # Callback that modifies the drawing elements
    @app.callback(Output('cyto-space', 'elements'), Input('fraction', 'value'))
    def update_fraction_edges(fraction):
        if fraction is None or fraction <= 0:
            return elements
        return delta_nodes_and_edges(cubes, fraction)

    app.layout = html.Div([
        html.P("OTree Index"),
        html.Div([
            html.Div(
                style={'width': '50%', 'display': 'inline'},
                children=['Sampling Fraction:', dcc.Input(id='fraction', type='number')]
            )
        ]),
        cyto.Cytoscape(
            id='cyto-space',
            elements=elements,
            layout={'name': LAYOUT_NAME, 'roots': '#root'},
            style={'width': '100%', 'height': '1000px'},
            stylesheet=[
                # Display cube max_weight for reach node
                {'selector': 'nodes',
                 'style': {
                     'label': 'data(label)'
                 }
                 },
                # Highlight sampled nodes and edges
                {'selector': '.sampled',
                 'style': {
                     'background-color': 'blue',
                     'line-color': 'blue'
                 }
                 }
            ]
        )
    ])

    app.run_server(debug=True)
