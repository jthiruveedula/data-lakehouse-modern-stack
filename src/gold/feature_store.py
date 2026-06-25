"""Vertex AI Feature Store integration — register and serve ML features from Gold."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class FeatureDefinition:
    name: str
    dtype: str  # "DOUBLE", "INT64", "STRING", "BOOL", "BYTES"
    description: str = ""
    labels: dict[str, str] = field(default_factory=dict)


@dataclass
class EntityTypeConfig:
    entity_type_id: str
    description: str
    features: list[FeatureDefinition]
    monitoring_interval_days: int = 1


@dataclass
class FeatureStoreConfig:
    project_id: str
    region: str
    featurestore_id: str
    entity_types: list[EntityTypeConfig]
    online_store_nodes: int = 1


class VertexFeatureStoreClient:
    """Manages Vertex AI Feature Store resources and ingests Gold layer features.

    Supports both batch ingestion (BigQuery → Feature Store) and
    online serving via the Vertex AI Feature Store API.
    """

    def __init__(self, config: FeatureStoreConfig) -> None:
        self.config = config
        self._admin_client: Any = None
        self._online_client: Any = None

    def _get_admin_client(self) -> Any:
        if self._admin_client is None:
            from google.cloud.aiplatform_v1 import FeaturestoreServiceClient

            self._admin_client = FeaturestoreServiceClient(
                client_options={"api_endpoint": f"{self.config.region}-aiplatform.googleapis.com"}
            )
        return self._admin_client

    @property
    def _featurestore_path(self) -> str:
        return (
            f"projects/{self.config.project_id}/"
            f"locations/{self.config.region}/"
            f"featurestores/{self.config.featurestore_id}"
        )

    def create_featurestore(self) -> None:
        """Provision the Feature Store if it doesn't already exist."""
        from google.cloud.aiplatform_v1.types import featurestore as fs_types

        client = self._get_admin_client()
        featurestore = fs_types.Featurestore(
            online_serving_config=fs_types.Featurestore.OnlineServingConfig(
                fixed_node_count=self.config.online_store_nodes
            )
        )
        parent = f"projects/{self.config.project_id}/locations/{self.config.region}"
        try:
            op = client.create_featurestore(
                parent=parent,
                featurestore=featurestore,
                featurestore_id=self.config.featurestore_id,
            )
            op.result(timeout=300)
            logger.info("Feature Store created: %s", self.config.featurestore_id)
        except Exception as exc:
            if "already exists" in str(exc).lower():
                logger.info("Feature Store already exists: %s", self.config.featurestore_id)
            else:
                raise

    def create_entity_types(self) -> None:
        """Create entity types and their feature definitions."""
        from google.cloud.aiplatform_v1.types import entity_type as et_types
        from google.cloud.aiplatform_v1.types import feature as feat_types

        client = self._get_admin_client()
        for et_config in self.config.entity_types:
            entity_type = et_types.EntityType(description=et_config.description)
            try:
                op = client.create_entity_type(
                    parent=self._featurestore_path,
                    entity_type=entity_type,
                    entity_type_id=et_config.entity_type_id,
                )
                op.result(timeout=120)
                logger.info("EntityType created: %s", et_config.entity_type_id)
            except Exception as exc:
                if "already exists" in str(exc).lower():
                    logger.debug("EntityType already exists: %s", et_config.entity_type_id)
                else:
                    raise

            entity_type_path = f"{self._featurestore_path}/entityTypes/{et_config.entity_type_id}"
            for feat_def in et_config.features:
                feature = feat_types.Feature(
                    description=feat_def.description,
                    value_type=getattr(feat_types.Feature.ValueType, feat_def.dtype),
                    labels=feat_def.labels,
                )
                try:
                    op = client.create_feature(
                        parent=entity_type_path,
                        feature=feature,
                        feature_id=feat_def.name,
                    )
                    op.result(timeout=120)
                    logger.info("Feature created: %s/%s", et_config.entity_type_id, feat_def.name)
                except Exception as exc:
                    if "already exists" in str(exc).lower():
                        logger.debug("Feature already exists: %s", feat_def.name)
                    else:
                        raise

    def ingest_from_bigquery(
        self,
        entity_type_id: str,
        bq_source_uri: str,
        entity_id_field: str,
        feature_time_field: str | None = None,
    ) -> None:
        """Batch-ingest features from a BigQuery table into the Feature Store."""
        from google.cloud.aiplatform_v1.types import featurestore_service

        client = self._get_admin_client()
        entity_type_path = f"{self._featurestore_path}/entityTypes/{entity_type_id}"

        import_request = featurestore_service.ImportFeatureValuesRequest(
            entity_type=entity_type_path,
            bigquery_source=featurestore_service.BigQuerySource(input_uri=bq_source_uri),
            entity_id_field=entity_id_field,
            feature_time_field=feature_time_field or "",
            worker_count=2,
        )
        op = client.import_feature_values(request=import_request)
        result = op.result(timeout=3600)
        logger.info(
            "Ingested features for entity_type=%s: imported=%d",
            entity_type_id,
            result.imported_entity_count,
        )

    def read_features(
        self,
        entity_type_id: str,
        entity_ids: list[str],
        feature_ids: list[str],
    ) -> list[dict[str, Any]]:
        """Online read of feature values for a list of entity IDs."""
        if self._online_client is None:
            from google.cloud.aiplatform_v1 import FeaturestoreOnlineServingServiceClient

            self._online_client = FeaturestoreOnlineServingServiceClient(
                client_options={"api_endpoint": f"{self.config.region}-aiplatform.googleapis.com"}
            )

        from google.cloud.aiplatform_v1.types import featurestore_online_service

        entity_type_path = f"{self._featurestore_path}/entityTypes/{entity_type_id}"
        selector = featurestore_online_service.FeatureSelector(
            id_matcher=featurestore_online_service.IdMatcher(ids=feature_ids)
        )

        results = []
        for entity_id in entity_ids:
            response = self._online_client.read_feature_values(
                featurestore_online_service.ReadFeatureValuesRequest(
                    entity_type=entity_type_path,
                    entity_id=entity_id,
                    feature_selector=selector,
                )
            )
            row: dict[str, Any] = {"entity_id": entity_id}
            for header, value in zip(
                response.header.feature_descriptors,
                response.entity_view.data,
                strict=True,
            ):
                row[header.id] = _extract_value(value)
            results.append(row)
        return results


def _extract_value(data_point: Any) -> Any:
    """Extract a typed Python value from a Vertex AI FeatureValue proto."""
    kind = data_point.value.WhichOneof("value")
    if kind is None:
        return None
    return getattr(data_point.value, kind)


# ── Predefined customer feature set ─────────────────────────────────────────


CUSTOMER_FEATURE_STORE_CONFIG = FeatureStoreConfig(
    project_id="",
    region="us-central1",
    featurestore_id="lakehouse_features",
    entity_types=[
        EntityTypeConfig(
            entity_type_id="customer",
            description="Customer-level aggregated features for ML models",
            features=[
                FeatureDefinition("lifetime_orders", "INT64", "Total orders placed"),
                FeatureDefinition("lifetime_revenue", "DOUBLE", "Sum of all order amounts"),
                FeatureDefinition("avg_order_value", "DOUBLE", "Mean order amount"),
                FeatureDefinition("days_since_last_order", "INT64", "Days since most recent order"),
                FeatureDefinition("customer_tenure_days", "INT64", "Days since first order"),
                FeatureDefinition("ltv_segment", "STRING", "Champion/Loyal/Promising/At Risk"),
                FeatureDefinition("tier", "STRING", "Customer tier (gold/silver/bronze)"),
                FeatureDefinition("region", "STRING", "Customer region"),
            ],
        )
    ],
)
