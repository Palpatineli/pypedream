from .task import Task
from datetime import datetime
try:
    import networkx as nx

    def _append_task(graph: nx.DiGraph, task: Task) -> str:
        if task.dependencies is None:
            graph.add_node(task.__name__, label=task.__fn__.__name__, time=task.__time__, input=True)
        else:
            graph.add_node(task.__name__, label=task.__fn__.__name__, time=task.__time__, input=False)
            for dependency in task.dependencies:
                child = _append_task(graph, dependency)
                graph.add_edge(child, task.__name__)
        return task.__name__

    def to_nx(task: Task) -> nx.DiGraph:
        """Convert a task tree to networkx Graph"""
        graph = nx.DiGraph()
        _append_task(graph, task)
        return graph

    def draw_nx(graph: nx.DiGraph, ax=None):
        input_nodes, internal_nodes, time_dict = list(), list(), dict()
        for key, value in graph.nodes.items():
            if value['input']:
                input_nodes.append(key)
            else:
                internal_nodes.append(key)
            time_dict[key] = datetime.fromtimestamp(value['time']).strftime("%m/%dT%H:%M")
        layout = nx.nx_agraph.graphviz_layout(graph, prog="dot")
        label_displaced = {key: (x, y - 25.0) for key, (x, y) in layout.items()}
        nx.draw_networkx(graph, layout, with_labels=True, ax=ax, nodelist=input_nodes, node_shape='h',
                         node_color="#268BD2FF", edge_color="#859900ff")
        nx.draw_networkx(graph, layout, with_labels=True, ax=ax, nodelist=internal_nodes, node_shape='o',
                         node_color="#2AA198FF", edge_color="#859900ff")
        nx.draw_networkx_labels(graph, label_displaced, time_dict)
except ImportError:
    def to_nx(task: Task) -> None:  # type: ignore
        raise NotImplementedError

    def draw_nx(graph: nx.DiGraph, ax=None) -> None:  # type: ignore
        raise NotImplementedError
