import multiprocessing

from proxy.config import config
from proxy.proxy_server import start_proxy
from tests.echo_app import run_app


if __name__ == "__main__":
    for ups in config.UPSTREAMS:
        multiprocessing.Process(target=run_app, args=(ups.host, ups.port)).start()

    # for _ in range(config.WORKERS):
    #     multiprocessing.Process(target=start_proxy).start()
    start_proxy()
