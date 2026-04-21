//! PostgresDatabase Node,infrastructure node providing durable key-value
//! storage backed by PostgreSQL.
//!
//! This node owns:
//! - Its K8s manifests (PVC, Deployment with Postgres + sidecar, Service)
//! - Its sidecar image (contains the DurableKV action handlers)
//! - The capabilities it implements (DurableKV, as a contract)
//!
//! The platform takes these manifests, replaces placeholders
//! (__INSTANCE_ID__, __NAMESPACE__), injects labels, and deploys.

use async_trait::async_trait;
use weft_core::node::{
    InfrastructureSpec, KubeManifest, ActionEndpoint,
};
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext};
use crate::{NodeResult, register_node};
use crate::infra_helpers::{infra_provision, infra_query_outputs};

const SIDECAR_PORT: u16 = 8090;

pub struct PostgresDatabaseNode;

impl PostgresDatabaseNode {
    fn infrastructure_spec() -> InfrastructureSpec {
        let pvc = serde_json::json!({
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": { "name": "__INSTANCE_ID__-data" },
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "resources": { "requests": { "storage": "1Gi" } }
            }
        });

        let deployment = serde_json::json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": { "name": "__INSTANCE_ID__" },
            "spec": {
                "replicas": 1,
                "selector": { "matchLabels": {} },
                "template": {
                    "metadata": { "labels": {} },
                    "spec": {
                        "containers": [
                            {
                                "name": "postgres",
                                "image": "postgres:17-alpine",
                                "ports": [{ "containerPort": 5432 }],
                                "env": [
                                    { "name": "POSTGRES_DB", "value": "durablekv" },
                                    { "name": "POSTGRES_USER", "value": "weavemind" },
                                    { "name": "POSTGRES_PASSWORD", "value": "weavemind" }
                                ],
                                "volumeMounts": [{
                                    "name": "pgdata",
                                    "mountPath": "/var/lib/postgresql/data",
                                    "subPath": "pgdata"
                                }],
                                "resources": {
                                    "requests": { "cpu": "100m", "memory": "256Mi" },
                                    "limits": { "cpu": "500m", "memory": "512Mi" }
                                },
                                "readinessProbe": {
                                    "exec": {
                                        "command": ["pg_isready", "-U", "weavemind", "-d", "durablekv"]
                                    },
                                    "initialDelaySeconds": 5,
                                    "periodSeconds": 5
                                }
                            },
                            {
                                "name": "sidecar",
                                "image": "__SIDECAR_IMAGE__",
                                "imagePullPolicy": "IfNotPresent",
                                "ports": [{ "containerPort": SIDECAR_PORT }],
                                "env": [
                                    { "name": "DATABASE_URL", "value": "postgres://weavemind:weavemind@localhost:5432/durablekv" },
                                    { "name": "PORT", "value": SIDECAR_PORT.to_string() }
                                ],
                                "resources": {
                                    "requests": { "cpu": "50m", "memory": "64Mi" },
                                    "limits": { "cpu": "200m", "memory": "128Mi" }
                                },
                                "readinessProbe": {
                                    "httpGet": { "path": "/health", "port": SIDECAR_PORT },
                                    "initialDelaySeconds": 10,
                                    "periodSeconds": 5
                                }
                            }
                        ],
                        "volumes": [{
                            "name": "pgdata",
                            "persistentVolumeClaim": { "claimName": "__INSTANCE_ID__-data" }
                        }]
                    }
                }
            }
        });

        let service = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": { "name": "__INSTANCE_ID__" },
            "spec": {
                "selector": {},
                "ports": [{
                    "name": "action",
                    "port": SIDECAR_PORT,
                    "targetPort": SIDECAR_PORT,
                    "protocol": "TCP"
                }]
            }
        });

        InfrastructureSpec {
            sidecarName: "postgres-database".to_string(),
            manifests: vec![
                KubeManifest { manifest: pvc },
                KubeManifest { manifest: deployment },
                KubeManifest { manifest: service },
            ],
            actionEndpoint: ActionEndpoint {
                port: SIDECAR_PORT,
                path: "/action".to_string(),
            },
        }
    }
}

#[async_trait]
impl Node for PostgresDatabaseNode {
    fn node_type(&self) -> &'static str {
        "PostgresDatabase"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Postgres Database",
            inputs: vec![],
            outputs: vec![
                PortDef::new("instanceId", "String", false),
                PortDef::new("endpointUrl", "String", false),
            ],
            features: NodeFeatures {
                isInfrastructure: true,
                infrastructureSpec: Some(Self::infrastructure_spec()),
                ..Default::default()
            },
            fields: vec![],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        if ctx.isInfraSetup {
            infra_provision(&ctx, &Self::infrastructure_spec()).await
        } else {
            infra_query_outputs(&ctx).await
        }
    }
}

register_node!(PostgresDatabaseNode);
