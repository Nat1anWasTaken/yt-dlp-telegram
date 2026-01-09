use dashmap::DashMap;
use rand::{distributions::Alphanumeric, Rng};
use std::{
    collections::HashMap,
    fmt,
    hash::{Hash, Hasher},
    sync::Arc,
};
use teloxide::types::{ChatId, MessageId};
use tokio::sync::Mutex;

#[derive(Clone, Debug, Eq)]
pub struct TaskId(String);

impl TaskId {
    pub fn new() -> Self {
        let value: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();
        Self(value)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn from_raw(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

impl PartialEq for TaskId {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Hash for TaskId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Clone, Debug)]
pub struct FormatMeta {
    pub ext: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FormatSelection {
    pub format_id: String,
    pub ext: Option<String>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub enum TaskState {
    WaitingFormat { formats: HashMap<String, FormatMeta> },
    PendingDownload { selection: FormatSelection },
    Downloading { selection: FormatSelection },
    Converting { selection: FormatSelection },
    Uploading { selection: FormatSelection },
    Finished { outcome: TaskOutcome },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TaskOutcome {
    Success,
    Failed(String),
}

#[derive(Clone, Debug)]
pub struct DownloadTask {
    pub id: TaskId,
    pub chat_id: ChatId,
    pub message_id: MessageId,
    pub url: String,
    pub title: Option<String>,
    pub state: TaskState,
}

impl DownloadTask {
    pub fn new(
        chat_id: ChatId,
        message_id: MessageId,
        url: String,
        title: Option<String>,
        formats: HashMap<String, FormatMeta>,
    ) -> Self {
        Self {
            id: TaskId::new(),
            chat_id,
            message_id,
            url,
            title,
            state: TaskState::WaitingFormat { formats },
        }
    }
}

#[derive(Clone, Default)]
pub struct TaskRegistry {
    inner: Arc<DashMap<TaskId, Arc<Mutex<DownloadTask>>>>,
}

impl TaskRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, task: DownloadTask) -> Arc<Mutex<DownloadTask>> {
        let id = task.id.clone();
        let task = Arc::new(Mutex::new(task));
        self.inner.insert(id, task.clone());
        task
    }

    pub fn get(&self, id: &TaskId) -> Option<Arc<Mutex<DownloadTask>>> {
        self.inner.get(id).map(|entry| entry.clone())
    }

    pub fn remove(&self, id: &TaskId) -> Option<Arc<Mutex<DownloadTask>>> {
        self.inner.remove(id).map(|(_, task)| task)
    }

    pub fn guard(&self, id: TaskId) -> TaskGuard {
        TaskGuard {
            registry: self.clone(),
            id,
        }
    }
}

pub struct TaskGuard {
    registry: TaskRegistry,
    id: TaskId,
}

impl Drop for TaskGuard {
    fn drop(&mut self) {
        let _ = self.registry.remove(&self.id);
    }
}

const TASK_CALLBACK_PREFIX: &str = "task:";

pub fn build_task_callback(id: &TaskId, format_id: &str) -> String {
    format!("{TASK_CALLBACK_PREFIX}{}:{format_id}", id.as_str())
}

pub fn parse_task_callback(data: &str) -> Option<(TaskId, String)> {
    let payload = data.strip_prefix(TASK_CALLBACK_PREFIX)?;
    let mut parts = payload.splitn(2, ':');
    let task_id = parts.next()?.trim();
    let format_id = parts.next()?.trim();
    if task_id.is_empty() || format_id.is_empty() {
        return None;
    }
    Some((TaskId::from_raw(task_id), format_id.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn callback_roundtrip() {
        let id = TaskId::from_raw("task123");
        let data = build_task_callback(&id, "140");
        let parsed = parse_task_callback(&data).unwrap();
        assert_eq!(parsed.0, id);
        assert_eq!(parsed.1, "140");
    }

    #[test]
    fn registry_insert_get_remove() {
        let registry = TaskRegistry::new();
        let mut formats = HashMap::new();
        formats.insert("18".to_string(), FormatMeta { ext: Some("mp4".into()) });
        let task = DownloadTask::new(ChatId(1), MessageId(1), "http://example.com".into(), None, formats);
        let id = task.id.clone();
        registry.insert(task);
        assert!(registry.get(&id).is_some());
        registry.remove(&id);
        assert!(registry.get(&id).is_none());
    }

    #[tokio::test]
    async fn guard_removes_on_drop() {
        let registry = TaskRegistry::new();
        let task = DownloadTask::new(ChatId(1), MessageId(1), "http://example.com".into(), None, HashMap::new());
        let id = task.id.clone();
        registry.insert(task);
        {
            let _guard = registry.guard(id.clone());
        }
        assert!(registry.get(&id).is_none());
    }
}
