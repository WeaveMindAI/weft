//! WhatsAppBridge Node, infrastructure node providing a WhatsApp connection
//! via Baileys (Node.js sidecar).
//!
//! This node owns:
//! - Its K8s manifests (PVC for auth state, Deployment with Node.js sidecar, Service)
//! - Its sidecar image (Node.js + Baileys, handles WhatsApp WebSocket)
//! - The capabilities it implements (WhatsApp messaging, as a contract)
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

pub struct WhatsAppBridgeNode;

impl WhatsAppBridgeNode {
    fn infrastructure_spec() -> InfrastructureSpec {
        // PVC for Baileys auth state persistence (survives pod restarts)
        let pvc = serde_json::json!({
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": { "name": "__INSTANCE_ID__-auth" },
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "resources": { "requests": { "storage": "100Mi" } }
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
                                "name": "whatsapp",
                                "image": "__SIDECAR_IMAGE__",
                                "imagePullPolicy": "IfNotPresent",
                                "ports": [{ "containerPort": SIDECAR_PORT }],
                                "env": [
                                    { "name": "PORT", "value": SIDECAR_PORT.to_string() },
                                    { "name": "AUTH_DIR", "value": "/data/auth" }
                                ],
                                "volumeMounts": [{
                                    "name": "authdata",
                                    "mountPath": "/data/auth"
                                }],
                                "resources": {
                                    "requests": { "cpu": "100m", "memory": "128Mi" },
                                    "limits": { "cpu": "500m", "memory": "512Mi" }
                                },
                                "readinessProbe": {
                                    "httpGet": { "path": "/health", "port": SIDECAR_PORT },
                                    "initialDelaySeconds": 5,
                                    "periodSeconds": 5
                                }
                            }
                        ],
                        "volumes": [{
                            "name": "authdata",
                            "persistentVolumeClaim": { "claimName": "__INSTANCE_ID__-auth" }
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
            sidecarName: "whatsapp-bridge".to_string(),
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
impl Node for WhatsAppBridgeNode {
    fn node_type(&self) -> &'static str {
        "WhatsAppBridge"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "WhatsApp Bridge",
            inputs: vec![],
            outputs: vec![
                PortDef::new("instanceId", "String", false),
                PortDef::new("endpointUrl", "String", false),
                PortDef::new("status", "String", false),
                PortDef::new("phoneNumber", "String", false),
                PortDef::new("jid", "String", false),
            ],
            features: NodeFeatures {
                isInfrastructure: true,
                infrastructureSpec: Some(Self::infrastructure_spec()),
                hasLiveData: true,
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

register_node!(WhatsAppBridgeNode);
