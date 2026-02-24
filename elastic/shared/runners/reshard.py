import asyncio
import copy

from esrally.driver.runner import Runner, runner_for, unwrap
from shared.utils.track import mandatory

"""
Runners for resharding indices and waiting for completion
"""


class StartReshard(Runner):
    async def __call__(self, es, params):
        index = mandatory(params, "index", self)
        split_shard_count = mandatory(params, "split_shard_count", self)
        body = {"index": index, "split_shard_count": split_shard_count}
        await es.perform_request(method="POST", path=f"/_internal/reshard/split", body=body)

    def __repr__(self, *args, **kwargs):
        return "reshard"


class WaitForReshard(Runner):
    async def __call__(self, es, params):
        index = mandatory(params, "index", self)
        wait_period = params.get("completion-recheck-wait-period", 1)

        resharding  = True
        while resharding:
            response = await es.perform_request(method="GET", path=f"/_internal/reshard/{index}/status")
            resharding = response.get("resharding", True)
            if resharding:
                await asyncio.sleep(wait_period)

    def __repr__(self, *args, **kwargs):
        return "wait-for-reshard"
