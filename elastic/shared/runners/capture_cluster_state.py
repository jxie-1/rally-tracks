# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import datetime
import json
import logging
import os

DEFAULT_INDICES = "logs-*,.ds-logs-*"


def _body(response):
    """Typed client calls return a response object that wraps the parsed body."""
    return getattr(response, "body", response)


def _shards_table(shards):
    """
    Renders _cat/shards output as whitespace separated columns, one shard copy per line. A relocating shard is
    reported on the node it is still on. Shards that are not assigned to a node are left out.
    """
    lines = ["index shard prirep state node docs store"]
    for shard in shards:
        node = shard.get("node")
        if not node:
            continue
        lines.append(
            " ".join(
                str(value) if value is not None else "-"
                for value in (shard["index"], shard["shard"], shard["prirep"], shard["state"], node.split()[0], shard.get("docs"), shard.get("store"))
            )
        )
    return "\n".join(lines) + "\n"


async def capture_cluster_state(es, params):
    """
    Writes a snapshot of the cluster state that matters for write-load balancing to a directory on the machine running
    the load driver, so it can be analysed after the benchmark without restarting Elasticsearch (the benchmark tooling stops
    it when the race ends). Meant to run right after a rollover, when the generation that has just been written to has its
    write load stats stored in the cluster state and its shards are still where they were during the load.

    Every request is best effort: a failure is logged and skipped, it never fails the race.

    Parameters:
    * label (mandatory): name of the snapshot, used as the sub directory name
    * directory (default: ~/write-load-swing): where to write the snapshot
    * indices (default: logs-*,.ds-logs-*): indices to capture shard placement and stats for
    """
    logger = logging.getLogger(__name__)
    label = params["label"]
    indices = params.get("indices", DEFAULT_INDICES)
    target = os.path.join(os.path.expanduser(params.get("directory", "~/write-load-swing")), label)
    os.makedirs(target, exist_ok=True)
    written = 0

    async def capture(filename, request, text=None):
        nonlocal written
        try:
            body = _body(await request())
            with open(os.path.join(target, filename), "w") as f:
                if text is not None:
                    f.write(text(body))
                else:
                    json.dump(body, f)
            written += 1
        except Exception:
            logger.warning("Could not capture [%s] for snapshot [%s]", filename, label, exc_info=True)

    # same shapes as the files collected by hand, so the same analysis can be used on both
    await capture(
        "shards.txt",
        lambda: es.cat.shards(index=indices, h=["index", "shard", "prirep", "state", "docs", "store", "node"], s=["index", "shard", "prirep"], format="json"),
        text=_shards_table,
    )
    await capture(
        "state_stats.json",
        lambda: es.cluster.state(
            metric="metadata", index=indices, filter_path=["metadata.indices.*.stats", "metadata.indices.*.*forecast*"], flat_settings=True
        ),
    )
    await capture(
        "data_streams.json",
        lambda: es.indices.get_data_stream(name="logs-*", filter_path=["data_streams.name", "data_streams.indices.index_name"]),
    )
    # live per shard copy write load, which includes the replicas
    await capture(
        "indices_stats.json",
        lambda: es.indices.stats(
            index=indices,
            metric="indexing",
            level="shards",
            filter_path=["indices.*.shards.*.routing", "indices.*.shards.*.indexing"],
        ),
    )
    await capture(
        "thread_pool.json",
        lambda: es.nodes.stats(metric="thread_pool", filter_path=["nodes.*.name", "nodes.*.thread_pool.write"]),
    )
    await capture("allocation.json", lambda: es.cat.allocation(format="json"))
    await capture("desired_balance.json", lambda: es.perform_request(method="GET", path="/_internal/desired_balance"))

    with open(os.path.join(target, "meta.json"), "w") as f:
        json.dump({"label": label, "captured_at": datetime.datetime.now(datetime.timezone.utc).isoformat(), "files": written}, f)

    return {"weight": written, "unit": "ops", "success": True}
