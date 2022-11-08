from dataclasses import dataclass
from datetime import datetime
from typing import Callable, List, Literal, Optional, Sequence, Union

from tqdm import tqdm

from feast.batch_feature_view import BatchFeatureView
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.infra.offline_stores.offline_store import OfflineStore
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.registry.base_registry import BaseRegistry
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.stream_feature_view import StreamFeatureView
from feast.utils import (
    _convert_arrow_to_proto,
    _get_column_names,
    _run_pyarrow_field_mapping,
)

from feast.infra.materialization.batch_materialization_engine import (
    BatchMaterializationEngine,
    MaterializationJob,
    MaterializationJobStatus,
    MaterializationTask,
)

from .spark_materialization_job import SparkMaterializationJob
from .livy import LivyOperator
import re
import json
import time
import math
from pydantic import StrictStr
from feast.infra.online_stores.contrib.cassandra_online_store.cassandra_online_store import CassandraOnlineStoreConfig

class SparkMaterializationEngineConfig(FeastConfigBaseModel):
    """Batch Materialization Engine config for local in-process engine"""

    type: Literal["spark"] = "spark"
    """ Type selector"""

    livy_url: StrictStr

    livy_conf: dict


class SparkMaterializationEngine(BatchMaterializationEngine):
    def update(
        self,
        project: str,
        views_to_delete: Sequence[
            Union[BatchFeatureView, StreamFeatureView, FeatureView]
        ],
        views_to_keep: Sequence[
            Union[BatchFeatureView, StreamFeatureView, FeatureView]
        ],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
    ):
        # Nothing to set up.
        pass

    def teardown_infra(
        self,
        project: str,
        fvs: Sequence[Union[BatchFeatureView, StreamFeatureView, FeatureView]],
        entities: Sequence[Entity],
    ):
        # Nothing to tear down.
        pass

    def __init__(
        self,
        *,
        repo_config: RepoConfig,
        offline_store: OfflineStore,
        online_store: OnlineStore,
        **kwargs,
    ):
        super().__init__(
            repo_config=repo_config,
            offline_store=offline_store,
            online_store=online_store,
            **kwargs,
        )

        self.batch_engine_config = repo_config.batch_engine
        self.livy_operator = LivyOperator(
            self.batch_engine_config.livy_url,
            self.batch_engine_config.livy_conf,
        )

    def materialize(
        self, registry, tasks: List[MaterializationTask]
    ) -> List[MaterializationJob]:
        return [
            self._materialize_one(
                registry,
                task.feature_view,
                task.start_time,
                task.end_time,
                task.project,
                task.tqdm_builder,
            )
            for task in tasks
        ]

    def _materialize_one(
        self,
        registry: BaseRegistry,
        feature_view: Union[BatchFeatureView, StreamFeatureView, FeatureView],
        start_date: datetime,
        end_date: datetime,
        project: str,
        tqdm_builder: Callable[[int], tqdm],
    ):
        online_store_config = self.repo_config.online_store
        if not isinstance(online_store_config, CassandraOnlineStoreConfig):
            raise CassandraInvalidConfig(E_CASSANDRA_UNEXPECTED_CONFIGURATION_CLASS)

        entities = []
        for entity_name in feature_view.entities:
            entities.append(registry.get_entity(entity_name, project))

        (
            join_key_columns,
            feature_name_columns,
            timestamp_field,
            created_timestamp_column,
        ) = _get_column_names(feature_view, entities)

        job_id = f"{feature_view.name}-{start_date}-{end_date}"

        try:

            offline_job = self.offline_store.pull_latest_from_table_or_query(
                config=self.repo_config,
                data_source=feature_view.batch_source,
                join_key_columns=join_key_columns,
                feature_name_columns=feature_name_columns,
                timestamp_field=timestamp_field,
                created_timestamp_column=created_timestamp_column,
                start_date=start_date,
                end_date=end_date,
            )

            sql = offline_job.to_sql()

            config_dict = {}

            online_store_config_dict = {}
            online_store_config_dict["host"] = online_store_config.hosts
            online_store_config_dict["port"] = online_store_config.port
            online_store_config_dict["keyspace"] = online_store_config.keyspace
            online_store_config_dict["type"] = "cassandra"

            materialization_dict = {}
            # materialization_dict["meta_uris"] = "thrift://ip-172-31-176-71.us-west-2.compute.internal:9083,thrift://ip-172-31-177-137.us-west-2.compute.internal:9083,thrift://ip-172-31-177-14.us-west-2.compute.internal:9083"
            materialization_dict["sql"] = re.sub("\\s+", " ", sql)
            materialization_dict["feature_view"] = feature_view.name
            materialization_dict["timestamp_field"] = feature_view.batch_source.timestamp_field
            materialization_dict["project"] = project
            materialization_dict["ttl"] = math.ceil(feature_view.ttl.total_seconds())
            materialization_dict["join_keys"] = {
                entity.name: entity.dtype.to_value_type().name
                for entity in feature_view.entity_columns if entity.name in join_key_columns
            }
            materialization_dict["features"] = {
                feature.name: feature.dtype.to_value_type().name
                for feature in feature_view.features if feature.name in feature_name_columns
            }
            materialization_dict["online_store_config"] = online_store_config_dict

            config_dict["materialization"] = materialization_dict

            spark_dict = {"app_name": f"feast_materialization_{feature_view.name}"}
            config_dict["spark"] = spark_dict

            config_str = json.dumps(config_dict)

            args = ["-c", re.sub("}", " }", config_str)]

            livy_id = self.livy_operator.submit(args)
            if livy_id is None:
                return SparkMaterializationJob(
                job_id=job_id, status=MaterializationJobStatus.ERROR
            )
            print(livy_id)

            while True:
                state = self.livy_operator.state_query(livy_id)
                print(state)
                if state == 'starting' or state == 'running':
                    time.sleep(5)
                    continue
                elif state == 'success':
                    return SparkMaterializationJob(
                        job_id=job_id, status=MaterializationJobStatus.SUCCEEDED
                    )
                elif state == 'dead':
                    return SparkMaterializationJob(
                        job_id=job_id, status=MaterializationJobStatus.ERROR
                    )
                elif state == 'killed':
                    return SparkMaterializationJob(
                        job_id=job_id, status=MaterializationJobStatus.CANCELLED
                    )
                else:
                    time.sleep(5)
                    continue

            return SparkMaterializationJob(
                job_id=job_id, status=MaterializationJobStatus.SUCCEEDED
            )
        except BaseException as e:
            return SparkMaterializationJob(
                job_id=job_id, status=MaterializationJobStatus.ERROR, error=e
            )
