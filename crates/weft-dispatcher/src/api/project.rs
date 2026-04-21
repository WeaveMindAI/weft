use axum::{extract::{Path, State}, Json};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::state::DispatcherState;

#[derive(Debug, Serialize, Deserialize)]
pub struct ProjectSummary {
    pub id: String,
    pub name: String,
    pub status: String,
}

pub async fn list(State(_state): State<DispatcherState>) -> Json<Vec<ProjectSummary>> {
    Json(Vec::new())
}

pub async fn register(State(_state): State<DispatcherState>, Json(_body): Json<Value>) -> Json<Value> {
    Json(serde_json::json!({ "status": "not_implemented" }))
}

pub async fn get(State(_state): State<DispatcherState>, Path(_id): Path<String>) -> Json<Value> {
    Json(serde_json::json!({ "status": "not_implemented" }))
}

pub async fn remove(State(_state): State<DispatcherState>, Path(_id): Path<String>) -> Json<Value> {
    Json(serde_json::json!({ "status": "not_implemented" }))
}

pub async fn run(State(_state): State<DispatcherState>, Path(_id): Path<String>, Json(_body): Json<Value>) -> Json<Value> {
    Json(serde_json::json!({ "status": "not_implemented" }))
}

pub async fn activate(State(_state): State<DispatcherState>, Path(_id): Path<String>) -> Json<Value> {
    Json(serde_json::json!({ "status": "not_implemented" }))
}

pub async fn deactivate(State(_state): State<DispatcherState>, Path(_id): Path<String>) -> Json<Value> {
    Json(serde_json::json!({ "status": "not_implemented" }))
}
