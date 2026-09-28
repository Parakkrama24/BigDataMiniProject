from prometheus_client import Counter, Gauge, start_http_server

# prometheus_client raises an error if the same metric name is registered twice,
# so we cache what we've created and hand back the existing object.
_metrics = {}


def counter(name: str, description: str) -> Counter:
    if name not in _metrics:
        _metrics[name] = Counter(name, description)
    return _metrics[name]


def gauge(name: str, description: str) -> Gauge:
    if name not in _metrics:
        _metrics[name] = Gauge(name, description)
    return _metrics[name]


def start_metrics_server(port: int) -> None:
    start_http_server(port)
