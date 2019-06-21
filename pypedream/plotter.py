from typing import Dict, List, Union, Optional
from .task import Task, TaskMixin, Input
from datetime import datetime
try:
    import networkx as nx

    def _append_task(graph: nx.DiGraph, task: TaskMixin) -> str:
        if isinstance(task, Input):
            graph.add_node(task.__name__, label=task.__loader__.__name__, time=task.__time__, ntype='input')
        elif isinstance(task, Task):
            graph.add_node(task.__name__, label=task.__fn__.__name__, time=task.__time__, ntype='internal')
            for dependency in task.dependencies:
                child = _append_task(graph, dependency)
                graph.add_edge(child, task.__name__)
        else:
            raise NotImplementedError(f"Unrecognized Node Type: {type(task)}")
        return task.__name__

    def to_nx(tasks: Union[Task, List[Task]]) -> nx.DiGraph:
        """Convert a task tree to networkx Graph"""
        graph = nx.DiGraph()
        for task in ([tasks] if isinstance(tasks, Task) else tasks):
            _append_task(graph, task)
            graph.nodes[task.__name__].update({"ntype": "output"})
        return graph

    EDGE_COLOR = "#5688c7ff"

    def draw_nx(graph: nx.DiGraph, ax=None, prog: Optional[str] = 'dot', args: tuple = ()):
        nodes: Dict[str, list] = {key: list() for key in ("input", "output", "internal")}
        time_dict = dict()
        for key, value in graph.nodes.items():
            nodes[value['ntype']].append(key)
            time_dict[key] = datetime.fromtimestamp(value['time']).strftime("%m/%dT%H:%M")
        layout = nx.nx_agraph.graphviz_layout(graph, prog=prog, args=args)
        label_displaced = {key: (x, y - 25.0) for key, (x, y) in layout.items()}
        nx.draw_networkx(graph, layout, with_labels=True, ax=ax, nodelist=nodes["input"], node_shape='h',
                         node_color="#FB9F89FF", edge_color=EDGE_COLOR)
        nx.draw_networkx(graph, layout, with_labels=True, ax=ax, nodelist=nodes["internal"], node_shape='o',
                         node_color="#92D5E6FF", edge_color=EDGE_COLOR)
        nx.draw_networkx(graph, layout, with_labels=True, ax=ax, nodelist=nodes["output"], node_shape='v',
                         node_color="#70EE9CFF", edge_color=EDGE_COLOR)
        nx.draw_networkx_labels(graph, label_displaced, time_dict)
except ImportError:
    def to_nx(task: Task) -> None:  # type: ignore
        raise NotImplementedError

    def draw_nx(graph: nx.DiGraph, ax=None) -> None:  # type: ignore
        raise NotImplementedError
