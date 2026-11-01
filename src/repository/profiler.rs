//! Compact, deterministic repository profiling with optional explicitly configured local
//! OpenAI-compatible enrichment. Repository files are never sent anywhere by default.
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    time::Duration,
};

const MAX_FILE_BYTES: u64 = 256 * 1024;
const MAX_FILES: usize = 10_000;
const MAX_TOTAL_BYTES: usize = 32 * 1024 * 1024;
const MAX_PROFILE_FILES: usize = 1_500;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RepositoryProfileDocument {
    pub schema_version: String,
    pub repository: RepositoryIdentity,
    pub project: ProjectSummary,
    pub languages: Vec<LanguageStat>,
    pub manifests: Vec<String>,
    pub tests: Vec<String>,
    pub directories: Vec<DirectoryProfile>,
    pub files: Vec<FileProfile>,
    pub model_categories: Vec<ModelRecommendation>,
    pub profiling: ProfilingMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RepositoryIdentity {
    pub name: String,
    pub root: PathBuf,
    pub revision: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectSummary {
    pub project_type: String,
    pub summary: String,
    pub confidence: f32,
    pub evidence: Vec<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LanguageStat {
    pub name: String,
    pub files: usize,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DirectoryProfile {
    pub path: String,
    pub responsibility: String,
    pub task_types: Vec<String>,
    pub confidence: f32,
    pub evidence: Vec<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileProfile {
    pub path: String,
    pub language: String,
    pub task_types: Vec<String>,
    pub confidence: f32,
    pub evidence: Vec<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelRecommendation {
    pub category: String,
    pub task_types: Vec<String>,
    pub evidence: Vec<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfilingMetadata {
    pub method: String,
    pub llm_provider: Option<String>,
    pub files_considered: usize,
    pub files_excluded: usize,
    pub truncated: bool,
}

#[derive(Debug, Clone)]
pub struct ProfileOptions {
    /// Local OpenAI-compatible URL, e.g. `http://127.0.0.1:1234/v1/chat/completions`.
    pub provider: Option<String>,
    pub model: Option<String>,
    /// Explicit user consent to send repository structure and file paths remotely.
    pub allow_remote: bool,
}

pub async fn profile(root: &Path, options: &ProfileOptions) -> Result<RepositoryProfileDocument> {
    anyhow::ensure!(
        options.provider.is_some() || options.model.is_none(),
        "--model requires --provider"
    );
    let repository = crate::repository::discover(root).await?;
    let file_paths = visible_files(&repository.root).await?;
    let mut languages = BTreeMap::<String, usize>::new();
    let mut manifests = Vec::new();
    let mut tests = Vec::new();
    let mut directories = BTreeMap::<String, Vec<String>>::new();
    let mut files = Vec::new();
    let mut bytes_seen = 0usize;
    let mut excluded = 0usize;
    let mut truncated = file_paths.len() > MAX_FILES;

    for relative in file_paths.iter().take(MAX_FILES) {
        if should_exclude(relative) {
            excluded += 1;
            continue;
        }
        let full = repository.root.join(relative);
        let metadata = match tokio::fs::symlink_metadata(&full).await {
            Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => metadata,
            _ => {
                excluded += 1;
                continue;
            }
        };
        if metadata.len() > MAX_FILE_BYTES
            || bytes_seen.saturating_add(metadata.len() as usize) > MAX_TOTAL_BYTES
        {
            excluded += 1;
            truncated = true;
            continue;
        }
        let content = tokio::fs::read(&full)
            .await
            .with_context(|| format!("read {}", relative.display()))?;
        if content.contains(&0) || std::str::from_utf8(&content).is_err() {
            excluded += 1;
            continue;
        }
        bytes_seen += content.len();
        let path = relative.to_string_lossy().replace('\\', "/");
        let language = language_for(&path);
        if language != "unknown" {
            *languages.entry(language.clone()).or_default() += 1;
        }
        if is_manifest(&path) {
            manifests.push(path.clone());
        }
        if is_test_path(&path) {
            tests.push(path.clone());
        }
        for parent in parent_directories(&path) {
            directories.entry(parent).or_default().push(path.clone());
        }
        if files.len() < MAX_PROFILE_FILES {
            let (task_types, evidence) = classify_file(&path, &language);
            files.push(FileProfile {
                path,
                language,
                task_types,
                confidence: 0.72,
                evidence,
            });
        } else {
            truncated = true;
        }
    }
    manifests.sort();
    tests.sort();
    let mut directory_profiles = directories
        .into_iter()
        .map(|(path, members)| {
            let (responsibility, tasks, evidence, confidence) = classify_directory(&path, &members);
            DirectoryProfile {
                path,
                responsibility,
                task_types: tasks,
                confidence,
                evidence,
            }
        })
        .collect::<Vec<_>>();
    directory_profiles.sort_by(|a, b| a.path.cmp(&b.path));
    let mut language_stats = languages
        .into_iter()
        .map(|(name, files)| LanguageStat { name, files })
        .collect::<Vec<_>>();
    language_stats.sort_by(|a, b| b.files.cmp(&a.files).then_with(|| a.name.cmp(&b.name)));
    let evidence = manifests.iter().take(6).cloned().collect::<Vec<_>>();
    let project_type = infer_project_type(&language_stats, &manifests);
    let summary = if language_stats.is_empty() {
        "Repository source could not be classified from visible files.".into()
    } else {
        format!(
            "{} project with {} profiled files across {} detected language categories.",
            project_type,
            files.len(),
            language_stats.len()
        )
    };
    let mut document = RepositoryProfileDocument {
        schema_version: "1.0".into(),
        repository: RepositoryIdentity {
            name: repository
                .root
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("repository")
                .to_owned(),
            root: repository.root.clone(),
            revision: repository.revision,
        },
        project: ProjectSummary {
            project_type,
            summary,
            confidence: if evidence.is_empty() { 0.45 } else { 0.78 },
            evidence,
        },
        languages: language_stats,
        manifests,
        tests,
        directories: directory_profiles,
        files,
        model_categories: vec![
            ModelRecommendation {
                category: "code_reasoning".into(),
                task_types: vec!["coding".into(), "debugging".into()],
                evidence: vec!["Detected source files".into()],
            },
            ModelRecommendation {
                category: "test_analysis".into(),
                task_types: vec!["testing".into()],
                evidence: vec!["Detected test paths".into()],
            },
            ModelRecommendation {
                category: "long_context_reasoning".into(),
                task_types: vec!["documentation".into(), "text_reasoning".into()],
                evidence: vec!["Repository-wide documentation and source structure".into()],
            },
        ],
        profiling: ProfilingMetadata {
            method: "deterministic".into(),
            llm_provider: None,
            files_considered: files_count(&file_paths, MAX_FILES),
            files_excluded: excluded,
            truncated,
        },
    };
    if let Some(provider) = &options.provider {
        let model = options
            .model
            .as_deref()
            .context("--model is required with --provider")?;
        let enrichment = llm_enrichment(provider, model, &document, options.allow_remote).await?;
        document.project.summary = enrichment;
        document.project.evidence.push(
            "OpenAI-compatible provider summary; structural profile remains locally derived".into(),
        );
        document.profiling.method = "deterministic+llm_summary".into();
        document.profiling.llm_provider = Some(provider.clone());
    }
    validate_profile(&document)?;
    Ok(document)
}

/// JSON Schema 2020-12 subset enforced by the typed decoder and semantic checks below.
pub const PROFILE_JSON_SCHEMA: &str = r#"{
  "$schema":"https://json-schema.org/draft/2020-12/schema","type":"object",
  "required":["schema_version","repository","project","languages","manifests","tests","directories","files","model_categories","profiling"],
  "properties":{"schema_version":{"const":"1.0"},"repository":{"type":"object","required":["name","root","revision"]},"project":{"type":"object","required":["project_type","summary","confidence","evidence"]},"languages":{"type":"array"},"manifests":{"type":"array","items":{"type":"string"}},"tests":{"type":"array","items":{"type":"string"}},"directories":{"type":"array"},"files":{"type":"array"},"model_categories":{"type":"array"},"profiling":{"type":"object"}},"additionalProperties":false
}"#;

pub fn validate_profile(profile: &RepositoryProfileDocument) -> Result<()> {
    anyhow::ensure!(
        profile.schema_version == "1.0",
        "unsupported profile schema_version"
    );
    anyhow::ensure!(
        !profile.repository.name.is_empty()
            && profile.repository.root.is_absolute()
            && !profile.repository.revision.is_empty(),
        "profile repository identity is incomplete"
    );
    anyhow::ensure!(
        (0.0..=1.0).contains(&profile.project.confidence),
        "project confidence must be between 0 and 1"
    );
    for item in &profile.files {
        anyhow::ensure!(
            (0.0..=1.0).contains(&item.confidence),
            "file confidence out of range"
        );
    }
    for item in &profile.directories {
        anyhow::ensure!(
            (0.0..=1.0).contains(&item.confidence),
            "directory confidence out of range"
        );
    }
    anyhow::ensure!(
        profile
            .model_categories
            .iter()
            .all(|item| !item.category.is_empty() && !item.task_types.is_empty()),
        "model recommendations require a category and task types"
    );
    const TASK_TYPES: &[&str] = &[
        "coding",
        "debugging",
        "testing",
        "documentation",
        "text_reasoning",
    ];
    anyhow::ensure!(
        profile.files.iter().all(|item| item
            .task_types
            .iter()
            .all(|t| TASK_TYPES.contains(&t.as_str())))
            && profile.directories.iter().all(|item| item
                .task_types
                .iter()
                .all(|t| TASK_TYPES.contains(&t.as_str())))
            && profile.model_categories.iter().all(|item| item
                .task_types
                .iter()
                .all(|t| TASK_TYPES.contains(&t.as_str()))),
        "profile contains an unknown task type"
    );
    Ok(())
}

async fn visible_files(root: &Path) -> Result<Vec<PathBuf>> {
    let output = crate::git::output(
        root,
        &[
            "ls-files",
            "--cached",
            "--others",
            "--exclude-standard",
            "-z",
        ],
        false,
    )
    .await?;
    anyhow::ensure!(
        output.status.success(),
        "git ls-files failed while applying .gitignore"
    );
    let mut files = output
        .stdout
        .split(|b| *b == 0)
        .filter(|b| !b.is_empty())
        .map(|bytes| PathBuf::from(String::from_utf8_lossy(bytes).as_ref()))
        .collect::<Vec<_>>();
    files.sort();
    files.dedup();
    Ok(files)
}
fn files_count(paths: &[PathBuf], limit: usize) -> usize {
    paths.len().min(limit)
}
fn should_exclude(path: &Path) -> bool {
    let parts = path
        .components()
        .filter_map(|c| c.as_os_str().to_str())
        .collect::<Vec<_>>();
    let lower = path.to_string_lossy().to_lowercase();
    const DIRS: &[&str] = &[
        ".git",
        ".aws",
        ".ssh",
        ".gnupg",
        "target",
        "node_modules",
        "vendor",
        "dist",
        "build",
        "out",
        ".venv",
        "venv",
        "__pycache__",
        ".next",
        "coverage",
        "bin",
        "obj",
        "secrets",
        "private",
    ];
    if parts
        .iter()
        .any(|p| DIRS.contains(&p.to_ascii_lowercase().as_str()))
    {
        return true;
    }
    let name = parts.last().copied().unwrap_or("");
    if name.starts_with(".env")
        || [
            "id_rsa",
            "id_ed25519",
            "credentials",
            "credentials.json",
            "secrets.json",
            "secret.json",
            "secret",
            "secrets",
            "*.pem",
        ]
        .iter()
        .any(|s| name.eq_ignore_ascii_case(s.trim_start_matches("*")))
    {
        return true;
    }
    let ext = Path::new(name)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    if [
        "png", "jpg", "jpeg", "gif", "webp", "ico", "pdf", "zip", "gz", "xz", "7z", "rar", "exe",
        "dll", "so", "dylib", "o", "a", "bin", "wasm", "mp3", "mp4", "ttf", "woff", "woff2",
        "lockb", "pem", "key", "p12", "pfx", "crt",
    ]
    .contains(&ext.as_str())
    {
        return true;
    }
    lower.ends_with(".lock")
        && !lower.ends_with("cargo.lock")
        && !lower.ends_with("package-lock.json")
}
fn language_for(path: &str) -> String {
    let ext = Path::new(path)
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    match ext.as_str() {
        "rs" => "Rust",
        "py" => "Python",
        "js" | "jsx" | "mjs" | "cjs" => "JavaScript",
        "ts" | "tsx" => "TypeScript",
        "go" => "Go",
        "java" => "Java",
        "kt" => "Kotlin",
        "c" | "h" => "C",
        "cc" | "cpp" | "hpp" => "C++",
        "cs" => "C#",
        "rb" => "Ruby",
        "php" => "PHP",
        "swift" => "Swift",
        "sh" => "Shell",
        "md" | "rst" => "Documentation",
        "toml" | "json" | "yaml" | "yml" => "Configuration",
        "html" | "css" | "scss" => "Web",
        "sql" => "SQL",
        _ => "unknown",
    }
    .into()
}
fn is_manifest(path: &str) -> bool {
    let name = Path::new(path)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("");
    [
        "Cargo.toml",
        "package.json",
        "pyproject.toml",
        "requirements.txt",
        "go.mod",
        "pom.xml",
        "build.gradle",
        "Gemfile",
        "composer.json",
        "Makefile",
        "CMakeLists.txt",
    ]
    .contains(&name)
        || name.ends_with(".sln")
}
fn is_test_path(path: &str) -> bool {
    let p = path.to_ascii_lowercase();
    p.split('/')
        .any(|s| ["tests", "test", "spec", "specs"].contains(&s))
        || p.contains("_test.")
        || p.contains(".test.")
        || p.contains(".spec.")
}
fn parent_directories(path: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = PathBuf::new();
    let components = Path::new(path).components().collect::<Vec<_>>();
    for c in components.iter().take(components.len().saturating_sub(1)) {
        current.push(c);
        out.push(current.to_string_lossy().replace('\\', "/"));
    }
    out
}
fn classify_file(path: &str, language: &str) -> (Vec<String>, Vec<String>) {
    let lower = path.to_ascii_lowercase();
    let mut tasks = BTreeSet::new();
    let mut evidence = Vec::new();
    if language != "unknown" && language != "Documentation" && language != "Configuration" {
        tasks.insert("coding".to_string());
        evidence.push(format!("file extension maps to {language}"));
    }
    if is_test_path(path) {
        tasks.insert("testing".into());
        evidence.push("path matches test naming convention".into());
    }
    if language == "Documentation" || lower.contains("readme") || lower.contains("docs/") {
        tasks.insert("documentation".into());
        evidence.push("documentation path or file type".into());
    }
    if ["issue", "adr", "design", "spec", "plan"]
        .iter()
        .any(|tag| lower.contains(tag))
        || language == "Documentation"
    {
        tasks.insert("text_reasoning".into());
    }
    if lower.contains("error")
        || lower.contains("debug")
        || lower.contains("log")
        || lower.contains("panic")
    {
        tasks.insert("debugging".into());
        evidence.push("path name suggests diagnostics".into());
    }
    if tasks.is_empty() {
        tasks.insert("text_reasoning".into());
        evidence.push("text or configuration file".into());
    }
    (
        tasks.into_iter().collect(),
        if evidence.is_empty() {
            vec!["repository-relative path and file extension".into()]
        } else {
            evidence
        },
    )
}
fn classify_directory(path: &str, members: &[String]) -> (String, Vec<String>, Vec<String>, f32) {
    let p = path.to_ascii_lowercase();
    let mut task = BTreeSet::new();
    let (responsibility, ev, confidence) = if p
        .split('/')
        .any(|x| ["tests", "test", "spec", "specs"].contains(&x))
    {
        task.insert("testing".into());
        (
            "tests and fixtures",
            "directory name matches test convention",
            0.93,
        )
    } else if p
        .split('/')
        .any(|x| ["docs", "doc", "documentation"].contains(&x))
    {
        task.extend(["documentation".into(), "text_reasoning".into()]);
        (
            "documentation",
            "directory name identifies documentation",
            0.95,
        )
    } else if p
        .split('/')
        .any(|x| ["src", "lib", "app", "pkg", "crates"].contains(&x))
    {
        task.extend(["coding".into(), "debugging".into()]);
        (
            "application or library source",
            "directory name identifies source code",
            0.88,
        )
    } else if p
        .split('/')
        .any(|x| ["scripts", "tools", "bin"].contains(&x))
    {
        task.insert("coding".into());
        (
            "developer tools and scripts",
            "directory name identifies tooling",
            0.78,
        )
    } else {
        let test_count = members.iter().filter(|m| is_test_path(m)).count();
        if test_count > 0 {
            task.insert("testing".into());
        } else {
            task.insert("coding".into());
        }
        (
            "repository files grouped by path",
            "inferred from member file paths",
            0.58,
        )
    };
    (
        responsibility.into(),
        task.into_iter().collect(),
        vec![ev.into()],
        confidence,
    )
}
fn infer_project_type(languages: &[LanguageStat], manifests: &[String]) -> String {
    if manifests.iter().any(|m| m.ends_with("Cargo.toml")) {
        "Rust application or library".into()
    } else if manifests.iter().any(|m| m.ends_with("package.json")) {
        "JavaScript or TypeScript application".into()
    } else if manifests
        .iter()
        .any(|m| m.ends_with("pyproject.toml") || m.ends_with("requirements.txt"))
    {
        "Python application or library".into()
    } else {
        languages
            .first()
            .map(|l| format!("{} software project", l.name))
            .unwrap_or_else(|| "General repository".into())
    }
}

async fn llm_enrichment(
    endpoint: &str,
    model: &str,
    profile: &RepositoryProfileDocument,
    allow_remote: bool,
) -> Result<String> {
    let url=endpoint.strip_prefix("http://").context("provider must be an http:// OpenAI-compatible endpoint; HTTPS endpoints are not supported by this local profiler")?;
    let (authority, path) = url.split_once('/').unwrap_or((url, "v1/chat/completions"));
    let (host, port) = authority
        .rsplit_once(':')
        .map(|(h, p)| (h, p.parse::<u16>().ok()))
        .unwrap_or((authority, None));
    let port = port.unwrap_or(80);
    let local = ["localhost", "127.0.0.1", "::1"].contains(&host);
    if !local && !allow_remote {
        bail!(
            "remote profiling provider rejected; pass --allow-remote to explicitly consent to sending repository structure and file paths"
        )
    }
    let addr = format!("{host}:{port}");
    let mut stream = tokio::net::TcpStream::connect(&addr)
        .await
        .with_context(|| format!("connect profiling provider {addr}"))?;
    let prompt = json_prompt(profile)?;
    let body = serde_json::json!({"model":model,"messages":[{"role":"system","content":"Summarize the supplied repository structure and suggest a concise project description. Return only JSON with a single string property summary. Do not infer model performance or scores."},{"role":"user","content":prompt}],"response_format":{"type":"json_object"},"temperature":0});
    let body = serde_json::to_vec(&body)?;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let request = format!(
        "POST /{} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n{}\r\n",
        path.trim_start_matches('/'),
        authority,
        body.len(),
        if let Ok(key) = std::env::var("SYNROUTE_PROFILE_API_KEY") {
            format!("Authorization: Bearer {key}\r\n")
        } else {
            String::new()
        }
    );
    stream.write_all(request.as_bytes()).await?;
    stream.write_all(&body).await?;
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(30), stream.read_to_end(&mut response))
        .await
        .context("profiling provider timed out")??;
    anyhow::ensure!(
        response.len() <= 1024 * 1024,
        "profiling response exceeds 1 MiB"
    );
    parse_llm_response(&response)
}

fn parse_llm_response(response: &[u8]) -> Result<String> {
    let split = response
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .context("invalid HTTP response")?
        + 4;
    let headers = String::from_utf8_lossy(&response[..split]);
    anyhow::ensure!(
        headers.lines().next().is_some_and(|l| l.contains(" 200 ")),
        "profiling provider returned non-success status"
    );
    let text =
        std::str::from_utf8(&response[split..]).context("profiling response is not UTF-8")?;
    let decoded: serde_json::Value =
        serde_json::from_str(text).context("decode provider JSON response")?;
    let content = decoded
        .pointer("/choices/0/message/content")
        .and_then(|v| v.as_str())
        .context("provider response is missing choices[0].message.content")?;
    let parsed: serde_json::Value =
        serde_json::from_str(content).context("provider summary is not constrained JSON")?;
    let summary = parsed
        .get("summary")
        .and_then(|v| v.as_str())
        .context("provider JSON lacks string summary")?;
    anyhow::ensure!(
        !summary.trim().is_empty() && summary.len() <= 2000,
        "provider summary is empty or too long"
    );
    Ok(summary.trim().to_owned())
}
fn json_prompt(profile: &RepositoryProfileDocument) -> Result<String> {
    Ok(serde_json::to_string(
        &serde_json::json!({"project_type":profile.project.project_type,"languages":profile.languages,"manifests":profile.manifests,"tests":profile.tests,"directories":profile.directories.iter().map(|d|(&d.path,&d.responsibility)).collect::<Vec<_>>(),"files":profile.files.iter().take(250).map(|f|(&f.path,&f.language)).collect::<Vec<_>>() }),
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn excludes_secrets_and_build_artifacts_and_maps_task_kinds() {
        for p in [
            ".env",
            "target/debug/app",
            "secrets.json",
            "node_modules/x.js",
            "id_rsa",
        ] {
            assert!(should_exclude(Path::new(p)), "{p}");
        }
        let (tasks, _) = classify_file("tests/cpu_test.rs", "Rust");
        assert!(tasks.contains(&"testing".into()));
        assert!(tasks.contains(&"coding".into()));
    }
    #[test]
    fn profile_schema_and_confidence_validation_are_applied() {
        let profile = RepositoryProfileDocument {
            schema_version: "1.0".into(),
            repository: RepositoryIdentity {
                name: "x".into(),
                root: "/tmp/x".into(),
                revision: "abc".into(),
            },
            project: ProjectSummary {
                project_type: "test".into(),
                summary: "test".into(),
                confidence: 0.5,
                evidence: vec![],
            },
            languages: vec![],
            manifests: vec![],
            tests: vec![],
            directories: vec![],
            files: vec![],
            model_categories: vec![ModelRecommendation {
                category: "code_reasoning".into(),
                task_types: vec!["coding".into()],
                evidence: vec![],
            }],
            profiling: ProfilingMetadata {
                method: "deterministic".into(),
                llm_provider: None,
                files_considered: 0,
                files_excluded: 0,
                truncated: false,
            },
        };
        validate_profile(&profile).unwrap();
        let mut invalid = profile;
        invalid.project.confidence = 2.0;
        assert!(validate_profile(&invalid).is_err());
    }

    #[tokio::test]
    async fn file_listing_respects_gitignore_for_untracked_files() {
        let dir = tempfile::tempdir().unwrap();
        std::process::Command::new("git")
            .args(["init", "-q"])
            .current_dir(dir.path())
            .status()
            .unwrap();
        std::fs::write(dir.path().join(".gitignore"), "ignored.txt\nignored-dir/\n").unwrap();
        std::fs::write(dir.path().join("visible.rs"), "fn main() {}\n").unwrap();
        std::fs::write(dir.path().join("ignored.txt"), "secret-ish\n").unwrap();
        std::fs::create_dir(dir.path().join("ignored-dir")).unwrap();
        std::fs::write(dir.path().join("ignored-dir/file.rs"), "ignored\n").unwrap();
        let paths = visible_files(&dir.path().canonicalize().unwrap())
            .await
            .unwrap();
        let strings = paths
            .iter()
            .map(|p| p.to_string_lossy().to_string())
            .collect::<Vec<_>>();
        assert!(strings.contains(&"visible.rs".to_owned()));
        assert!(!strings.iter().any(|p| p.starts_with("ignored")));
    }

    #[tokio::test]
    async fn remote_provider_requires_explicit_consent_before_network_access() {
        let profile = RepositoryProfileDocument {
            schema_version: "1.0".into(),
            repository: RepositoryIdentity {
                name: "x".into(),
                root: "/tmp/x".into(),
                revision: "abc".into(),
            },
            project: ProjectSummary {
                project_type: "test".into(),
                summary: "test".into(),
                confidence: 0.5,
                evidence: vec![],
            },
            languages: vec![],
            manifests: vec![],
            tests: vec![],
            directories: vec![],
            files: vec![],
            model_categories: vec![],
            profiling: ProfilingMetadata {
                method: "deterministic".into(),
                llm_provider: None,
                files_considered: 0,
                files_excluded: 0,
                truncated: false,
            },
        };
        let result = llm_enrichment(
            "http://example.invalid/v1/chat/completions",
            "model",
            &profile,
            false,
        )
        .await;
        assert!(result.unwrap_err().to_string().contains("--allow-remote"));
    }

    #[test]
    fn compatible_provider_json_is_parsed_and_rejected_when_invalid() {
        let body = serde_json::json!({"choices":[{"message":{"content":"{\"summary\":\"Rust emulator\"}"}}]}).to_string();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        assert_eq!(
            parse_llm_response(response.as_bytes()).unwrap(),
            "Rust emulator"
        );
        assert!(parse_llm_response(b"HTTP/1.1 200 OK\r\n\r\nnot json").is_err());
        assert!(parse_llm_response(b"HTTP/1.1 400 Bad Request\r\n\r\n{}").is_err());
    }
}
