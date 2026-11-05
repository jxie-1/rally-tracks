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

import json
from unittest import mock

import pytest
from shared.runners.capture_cluster_state import capture_cluster_state
from tests import as_future

SHARDS = [
    {"index": ".ds-logs-nginx.access-default-000001", "shard": "0", "prirep": "p", "state": "STARTED", "docs": "120", "store": "1mb", "node": "elasticsearch-1"},
    # a relocating shard is listed on the node it is still on, followed by the target
    {"index": ".ds-logs-nginx.access-default-000001", "shard": "0", "prirep": "r", "state": "RELOCATING", "docs": "120", "store": "1mb", "node": "elasticsearch-2 -> 10.0.0.3 AbC elasticsearch-3"},
    # unassigned shards have no node and are left out
    {"index": ".ds-logs-nginx.access-default-000002", "shard": "0", "prirep": "p", "state": "UNASSIGNED", "docs": None, "store": None, "node": None},
]


def _es(failing=None):
    es = mock.MagicMock()
    es.cat.shards.return_value = as_future(SHARDS)
    es.cluster.state.return_value = as_future({"metadata": {"indices": {"idx": {"stats": {"write_load": {"loads": [1.5]}}}}}})
    es.indices.get_data_stream.return_value = as_future({"data_streams": [{"name": "logs-nginx.access-default"}]})
    es.indices.stats.return_value = as_future({"indices": {}})
    es.nodes.stats.return_value = as_future({"nodes": {}})
    es.cat.allocation.return_value = as_future([{"node": "elasticsearch-1"}])
    es.perform_request.return_value = as_future({"stats": {}})
    if failing:
        target = es
        for part in failing.split("."):
            target = getattr(target, part)
        target.side_effect = RuntimeError("boom")
    return es


@pytest.mark.asyncio
async def test_capture_writes_snapshot(tmp_path):
    result = await capture_cluster_state(_es(), {"label": "after-step-2", "directory": str(tmp_path)})

    snapshot = tmp_path / "after-step-2"
    assert result == {"weight": 7, "unit": "ops", "success": True}
    assert sorted(p.name for p in snapshot.iterdir()) == sorted(
        [
            "shards.txt",
            "state_stats.json",
            "data_streams.json",
            "indices_stats.json",
            "thread_pool.json",
            "allocation.json",
            "desired_balance.json",
            "meta.json",
        ]
    )
    assert json.loads((snapshot / "state_stats.json").read_text())["metadata"]["indices"]["idx"]["stats"]["write_load"]["loads"] == [1.5]
    assert json.loads((snapshot / "meta.json").read_text())["label"] == "after-step-2"


@pytest.mark.asyncio
async def test_shards_table_has_one_row_per_assigned_copy(tmp_path):
    await capture_cluster_state(_es(), {"label": "s", "directory": str(tmp_path)})

    lines = (tmp_path / "s" / "shards.txt").read_text().splitlines()
    assert lines[0] == "index shard prirep state node docs store"
    assert lines[1] == ".ds-logs-nginx.access-default-000001 0 p STARTED elasticsearch-1 120 1mb"
    # relocating shards are reported on their current node, and the unassigned one is left out
    assert lines[2] == ".ds-logs-nginx.access-default-000001 0 r RELOCATING elasticsearch-2 120 1mb"
    assert len(lines) == 3


@pytest.mark.asyncio
async def test_failing_request_does_not_fail_the_capture(tmp_path):
    es = _es(failing="cluster.state")

    result = await capture_cluster_state(es, {"label": "s", "directory": str(tmp_path)})

    assert result["success"] is True
    assert result["weight"] == 6
    assert not (tmp_path / "s" / "state_stats.json").exists()
    assert (tmp_path / "s" / "shards.txt").exists()
