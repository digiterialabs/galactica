use std::env;
use std::fs;
use std::fs::File;
use std::io;
use std::net::{IpAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};
use galactica_artifact::{
    LocalModelRegistry, ModelRegistry, resolve_variant, runtime_preference_score,
};
use galactica_common::proto::common::v1::{
    AcceleratorType, CpuArch, ModelManifest, NodeCapabilities, OsType,
};
use galactica_node_agent::DefaultHardwareDetector;
use reqwest::header::{ACCEPT, AUTHORIZATION, HeaderMap, HeaderValue, USER_AGENT};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use zip::ZipArchive;

const DEFAULT_MODEL: &str = "qwen3.5-4b";

#[derive(Parser, Debug)]
#[command(name = "galactica-cli", version, about)]
struct Cli {
    /// Repository root. Defaults to the current directory or an ancestor containing Cargo.toml.
    #[arg(long)]
    repo_root: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Show the current machine profile and recommended runtime/configuration.
    Detect(ModelArgs),
    /// Check prerequisites for the detected machine and selected model.
    Doctor(ModelArgs),
    /// Prepare local state and print the next commands for this host.
    Install(InstallArgs),
    /// Mint and save a bootstrap enrollment token.
    Token(TokenArgs),
    /// Install galactica-cli into a user-local bin directory and create a repo-bound launcher.
    SelfInstall(SelfInstallArgs),
    /// Start a Galactica service using the checked-in dev configuration.
    Up(UpArgs),
}

#[derive(Args, Debug, Clone)]
struct ModelArgs {
    /// Model ID used for runtime recommendation.
    #[arg(long, default_value = DEFAULT_MODEL)]
    model: String,

    /// Print machine-readable JSON.
    #[arg(long)]
    json: bool,
}

#[derive(Args, Debug, Clone)]
struct InstallArgs {
    /// Model ID used for runtime recommendation.
    #[arg(long, default_value = DEFAULT_MODEL)]
    model: String,

    /// Print machine-readable JSON.
    #[arg(long)]
    json: bool,

    /// Plan installation steps without executing them.
    #[arg(long)]
    dry_run: bool,

    /// Reinstall runtime assets even if they already exist locally.
    #[arg(long)]
    force: bool,
}

#[derive(Args, Debug, Clone)]
struct TokenArgs {
    #[command(subcommand)]
    action: TokenAction,
}

#[derive(Subcommand, Debug, Clone)]
enum TokenAction {
    /// Mint an enrollment token from the checked-in control plane config.
    Mint {
        /// Override token TTL in seconds.
        #[arg(long)]
        ttl_seconds: Option<u64>,

        /// Print machine-readable JSON.
        #[arg(long)]
        json: bool,
    },
}

#[derive(Args, Debug, Clone)]
struct UpArgs {
    #[command(subcommand)]
    target: UpTarget,
}

#[derive(Subcommand, Debug, Clone)]
enum UpTarget {
    /// Start the control plane.
    ControlPlane {
        /// Print the resolved command without executing it.
        #[arg(long)]
        dry_run: bool,
    },
    /// Start the gateway.
    Gateway {
        /// Print the resolved command without executing it.
        #[arg(long)]
        dry_run: bool,
    },
    /// Start the node agent using the current machine profile.
    Node {
        /// Model ID used for runtime recommendation.
        #[arg(long, default_value = DEFAULT_MODEL)]
        model: String,

        /// Enrollment token. Falls back to GALACTICA_ENROLLMENT_TOKEN or var/dev/enrollment-token.txt.
        #[arg(long)]
        token: Option<String>,

        /// Control plane address override.
        #[arg(long)]
        control_plane_addr: Option<String>,

        /// Agent endpoint override.
        #[arg(long)]
        agent_endpoint: Option<String>,

        /// Print the resolved command without executing it.
        #[arg(long)]
        dry_run: bool,
    },
}

#[derive(Debug, Clone, Serialize)]
struct DetectionReport {
    repo_root: String,
    model: String,
    hostname: String,
    os: String,
    cpu_arch: String,
    execution_pool: String,
    accelerators: Vec<String>,
    runtime_backends: Vec<String>,
    recommended_runtime: Option<String>,
    recommended_quantization: Option<String>,
    recommended_node_config: String,
    detected_local_ip: String,
    suggested_agent_endpoint: String,
    notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct DoctorCheck {
    name: String,
    ok: bool,
    required: bool,
    detail: String,
}

#[derive(Debug, Clone, Serialize)]
struct DoctorReport {
    detection: DetectionReport,
    checks: Vec<DoctorCheck>,
    ready: bool,
    blocking_checks: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct InstallReport {
    detection: DetectionReport,
    actions: Vec<InstallAction>,
    prepared_paths: Vec<String>,
    ready: bool,
    blocking_checks: Vec<String>,
    next_steps: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct TokenMintReport {
    token: String,
    token_file: String,
}

#[derive(Args, Debug, Clone)]
struct SelfInstallArgs {
    /// Override the install root. The binary will be placed in <root>/bin.
    #[arg(long)]
    root: Option<PathBuf>,

    /// Reinstall even if galactica-cli is already present.
    #[arg(long)]
    force: bool,

    /// Print the resolved commands without executing them.
    #[arg(long)]
    dry_run: bool,

    /// Print machine-readable JSON.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Clone, Serialize)]
struct InstallAction {
    name: String,
    status: String,
    detail: String,
}

#[derive(Debug, Clone, Deserialize)]
struct GithubRelease {
    tag_name: String,
    assets: Vec<GithubAsset>,
}

#[derive(Debug, Clone, Deserialize)]
struct GithubAsset {
    name: String,
    browser_download_url: String,
}

#[derive(Debug, Clone, Serialize)]
struct SelfInstallReport {
    install_root: String,
    bin_dir: String,
    binary_path: String,
    launcher_path: String,
    install_method: String,
    on_path: bool,
    path_hint: String,
    actions: Vec<InstallAction>,
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<()> {
    let cli = Cli::parse();
    let repo_root = find_repo_root(cli.repo_root)?;

    match cli.command {
        Commands::Detect(args) => cmd_detect(&repo_root, args).await,
        Commands::Doctor(args) => cmd_doctor(&repo_root, args).await,
        Commands::Install(args) => cmd_install(&repo_root, args).await,
        Commands::Token(args) => cmd_token(&repo_root, args).await,
        Commands::SelfInstall(args) => cmd_self_install(&repo_root, args).await,
        Commands::Up(args) => cmd_up(&repo_root, args).await,
    }
}

async fn cmd_detect(repo_root: &Path, args: ModelArgs) -> Result<()> {
    let report = detect_host(repo_root, &args.model).await?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }

    println!("Host: {}", report.hostname);
    println!("Platform: {} {}", report.os, report.cpu_arch);
    println!("Pool: {}", report.execution_pool);
    println!("Accelerators: {}", join_or_na(&report.accelerators));
    println!("Backends: {}", join_or_na(&report.runtime_backends));
    println!(
        "Recommended runtime: {}",
        report.recommended_runtime.as_deref().unwrap_or("n/a")
    );
    println!(
        "Recommended quantization: {}",
        report.recommended_quantization.as_deref().unwrap_or("n/a")
    );
    println!("Node config: {}", report.recommended_node_config);
    println!(
        "Suggested agent endpoint: {}",
        report.suggested_agent_endpoint
    );
    if !report.notes.is_empty() {
        println!("Notes:");
        for note in &report.notes {
            println!("- {note}");
        }
    }
    Ok(())
}

async fn cmd_doctor(repo_root: &Path, args: ModelArgs) -> Result<()> {
    let report = build_doctor_report(repo_root, &args.model).await?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }

    println!("Ready: {}", if report.ready { "yes" } else { "no" });
    println!("Host: {}", report.detection.hostname);
    println!("Execution pool: {}", report.detection.execution_pool);
    println!(
        "Recommended runtime: {}",
        report
            .detection
            .recommended_runtime
            .as_deref()
            .unwrap_or("n/a")
    );
    for check in &report.checks {
        let state = if check.ok {
            "ok"
        } else if check.required {
            "fail"
        } else {
            "warn"
        };
        println!("[{state}] {}: {}", check.name, check.detail);
    }
    if !report.blocking_checks.is_empty() {
        println!("Blocking:");
        for blocking in &report.blocking_checks {
            println!("- {blocking}");
        }
        bail!("doctor found missing required prerequisites");
    }
    Ok(())
}

async fn cmd_install(repo_root: &Path, args: InstallArgs) -> Result<()> {
    let detection = detect_host(repo_root, &args.model).await?;
    let var_dev = repo_root.join("var/dev");
    fs::create_dir_all(&var_dev)
        .with_context(|| format!("failed to create {}", var_dev.display()))?;
    let mut actions = vec![InstallAction {
        name: "prepare var/dev".to_string(),
        status: "ok".to_string(),
        detail: format!("ensured {}", var_dev.display()),
    }];

    match detection.recommended_runtime.as_deref() {
        Some("mlx") => install_mlx_runtime(repo_root, args.dry_run, args.force, &mut actions)?,
        Some("llama.cpp") | Some("llamacpp") => {
            install_llama_cpp_runtime(
                repo_root,
                &detection,
                args.dry_run,
                args.force,
                &mut actions,
            )
            .await?
        }
        Some("vllm") => actions.push(InstallAction {
            name: "runtime install".to_string(),
            status: "manual".to_string(),
            detail: "automatic vLLM installation is not implemented yet; install vllm manually"
                .to_string(),
        }),
        Some("onnxruntime") | Some("onnx") => actions.push(InstallAction {
            name: "runtime install".to_string(),
            status: "manual".to_string(),
            detail: "automatic ONNX Runtime installation is not implemented yet".to_string(),
        }),
        _ => actions.push(InstallAction {
            name: "runtime install".to_string(),
            status: "skipped".to_string(),
            detail: "no recommended runtime could be determined for this host".to_string(),
        }),
    }

    let report = build_doctor_report(repo_root, &args.model).await?;
    let install_report = InstallReport {
        detection: report.detection.clone(),
        actions,
        prepared_paths: vec![var_dev.display().to_string()],
        ready: report.ready,
        blocking_checks: report.blocking_checks.clone(),
        next_steps: next_steps(&report.detection),
    };
    fs::write(
        var_dev.join("install-report.json"),
        serde_json::to_vec_pretty(&install_report)?,
    )
    .with_context(|| {
        format!(
            "failed to write {}",
            var_dev.join("install-report.json").display()
        )
    })?;

    if args.json {
        println!("{}", serde_json::to_string_pretty(&install_report)?);
    } else {
        println!("Actions:");
        for action in &install_report.actions {
            println!("- [{}] {}: {}", action.status, action.name, action.detail);
        }
        println!("Prepared:");
        for path in &install_report.prepared_paths {
            println!("- {path}");
        }
        println!("Ready: {}", if install_report.ready { "yes" } else { "no" });
        if !install_report.blocking_checks.is_empty() {
            println!("Blocking:");
            for blocking in &install_report.blocking_checks {
                println!("- {blocking}");
            }
        }
        println!("Recommended next steps:");
        for step in &install_report.next_steps {
            println!("- {step}");
        }
    }

    if !args.dry_run && !report.ready {
        bail!("install prepared local state, but prerequisites are still missing");
    }
    Ok(())
}

async fn cmd_token(repo_root: &Path, args: TokenArgs) -> Result<()> {
    match args.action {
        TokenAction::Mint { ttl_seconds, json } => {
            let token = mint_enrollment_token(repo_root, ttl_seconds)?;
            let token_file = repo_root.join("var/dev/enrollment-token.txt");
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&TokenMintReport {
                        token,
                        token_file: token_file.display().to_string(),
                    })?
                );
            } else {
                println!("{token}");
            }
            Ok(())
        }
    }
}

async fn cmd_self_install(repo_root: &Path, args: SelfInstallArgs) -> Result<()> {
    let install_root = args.root.unwrap_or(default_install_root()?);
    let bin_dir = install_root.join("bin");
    let binary_name = installed_binary_name();
    let binary_path = bin_dir.join(binary_name);
    let launcher_path = launcher_path(&bin_dir);
    let mut install_method = "release".to_string();

    let mut actions = Vec::new();
    actions.push(InstallAction {
        name: "resolve install root".to_string(),
        status: "ok".to_string(),
        detail: install_root.display().to_string(),
    });

    let cargo_install_args = cargo_install_args(repo_root, &install_root, args.force);
    if args.dry_run {
        if let Ok(api_url) = galactica_cli_release_api_url(repo_root) {
            let target = galactica_cli_target_triple(env::consts::OS, env::consts::ARCH)
                .unwrap_or("unsupported");
            actions.push(InstallAction {
                name: "download release asset".to_string(),
                status: "planned".to_string(),
                detail: format!("latest release from {api_url} matching {target}"),
            });
        }
        actions.push(InstallAction {
            name: "cargo install".to_string(),
            status: "planned".to_string(),
            detail: format!("fallback: cargo {}", shell_join(&cargo_install_args)),
        });
        actions.push(InstallAction {
            name: "create launcher".to_string(),
            status: "planned".to_string(),
            detail: launcher_path.display().to_string(),
        });
    } else {
        fs::create_dir_all(&bin_dir)
            .with_context(|| format!("failed to create {}", bin_dir.display()))?;
        match install_cli_from_release_asset(repo_root, &bin_dir, &binary_path, args.force).await {
            Ok(detail) => actions.push(InstallAction {
                name: "download release asset".to_string(),
                status: "ok".to_string(),
                detail,
            }),
            Err(release_error) => {
                install_method = "cargo".to_string();
                actions.push(InstallAction {
                    name: "download release asset".to_string(),
                    status: "warning".to_string(),
                    detail: release_error.to_string(),
                });
                run_cargo_install(repo_root, &cargo_install_args)?;
                actions.push(InstallAction {
                    name: "cargo install".to_string(),
                    status: "ok".to_string(),
                    detail: format!("installed {}", binary_path.display()),
                });
            }
        }
        write_repo_launcher(repo_root, &binary_path, &launcher_path)?;
        actions.push(InstallAction {
            name: "create launcher".to_string(),
            status: "ok".to_string(),
            detail: format!("wrote {}", launcher_path.display()),
        });
    }

    let on_path = path_contains_dir(&bin_dir);
    let report = SelfInstallReport {
        install_root: install_root.display().to_string(),
        bin_dir: bin_dir.display().to_string(),
        binary_path: binary_path.display().to_string(),
        launcher_path: launcher_path.display().to_string(),
        install_method,
        on_path,
        path_hint: path_hint(&bin_dir),
        actions,
    };

    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!("Installed binary: {}", report.binary_path);
        println!("Launcher: {}", report.launcher_path);
        println!("Install method: {}", report.install_method);
        println!("PATH ready: {}", if report.on_path { "yes" } else { "no" });
        for action in &report.actions {
            println!("- [{}] {}: {}", action.status, action.name, action.detail);
        }
        if !report.on_path {
            println!("Add to PATH:");
            println!("{}", report.path_hint);
        }
        println!("Use:");
        println!("galactica detect");
        println!("galactica doctor");
        println!("galactica install");
    }
    Ok(())
}

async fn cmd_up(repo_root: &Path, args: UpArgs) -> Result<()> {
    match args.target {
        UpTarget::ControlPlane { dry_run } => run_workspace_command(
            repo_root,
            "galactica-control-plane",
            vec![
                "--config".to_string(),
                repo_root
                    .join("config/dev/control-plane.toml")
                    .display()
                    .to_string(),
            ],
            dry_run,
            false,
            &[],
        ),
        UpTarget::Gateway { dry_run } => run_workspace_command(
            repo_root,
            "galactica-gateway",
            vec![
                "--config".to_string(),
                repo_root
                    .join("config/dev/gateway.toml")
                    .display()
                    .to_string(),
            ],
            dry_run,
            false,
            &[],
        ),
        UpTarget::Node {
            model,
            token,
            control_plane_addr,
            agent_endpoint,
            dry_run,
        } => {
            let detection = detect_host(repo_root, &model).await?;
            let token = resolve_enrollment_token(repo_root, token)?;
            let control_plane_addr = control_plane_addr
                .or_else(|| env::var("GALACTICA_CONTROL_PLANE_ADDR").ok())
                .unwrap_or_else(|| "http://127.0.0.1:9090".to_string());
            let agent_endpoint = agent_endpoint
                .or_else(|| env::var("GALACTICA_AGENT_ENDPOINT").ok())
                .unwrap_or_else(|| detection.suggested_agent_endpoint.clone());
            let mut args = vec![
                "--config".to_string(),
                repo_root
                    .join(&detection.recommended_node_config)
                    .display()
                    .to_string(),
                "--enrollment-token".to_string(),
                token,
                "--control-plane-addr".to_string(),
                control_plane_addr,
                "--agent-endpoint".to_string(),
                agent_endpoint,
            ];
            if let Some(runtime) = &detection.recommended_runtime {
                args.push("--runtime-backend".to_string());
                args.push(runtime.clone());
            }
            let env_overrides = runtime_env_overrides(repo_root, &detection);
            run_workspace_command(
                repo_root,
                "galactica-node-agent",
                args,
                dry_run,
                false,
                &env_overrides,
            )
        }
    }
}

async fn detect_host(repo_root: &Path, model: &str) -> Result<DetectionReport> {
    let mut detector = DefaultHardwareDetector::default();
    let snapshot = detector.detect_snapshot();
    let mut capabilities = snapshot.capabilities.clone();
    add_repo_runtime_backends(repo_root, &mut capabilities);
    let registry = LocalModelRegistry::new(repo_root.join("models"));
    let manifest = registry.get_model_manifest(model).await.ok();
    let variant = manifest
        .as_ref()
        .and_then(|manifest| resolve_variant(manifest, &capabilities).ok());
    let recommended_runtime = variant
        .as_ref()
        .map(|variant| variant.runtime.clone())
        .or_else(|| fallback_runtime(&capabilities));
    let recommended_quantization = variant.as_ref().map(|variant| variant.quantization.clone());
    let detected_local_ip = detect_local_ip().unwrap_or_else(|| "127.0.0.1".to_string());
    let node_config = default_node_config_for_capabilities(&capabilities);
    let notes = build_notes(model, manifest.as_ref(), &capabilities, variant.as_ref());

    Ok(DetectionReport {
        repo_root: repo_root.display().to_string(),
        model: model.to_string(),
        hostname: snapshot.hostname,
        os: os_label(capabilities.os).to_string(),
        cpu_arch: arch_label(capabilities.cpu_arch).to_string(),
        execution_pool: pool_label(&capabilities),
        accelerators: accelerator_labels(&capabilities),
        runtime_backends: capabilities.runtime_backends.clone(),
        recommended_runtime,
        recommended_quantization,
        recommended_node_config: node_config.display().to_string(),
        suggested_agent_endpoint: format!("http://{}:50061", detected_local_ip),
        detected_local_ip,
        notes,
    })
}

async fn build_doctor_report(repo_root: &Path, model: &str) -> Result<DoctorReport> {
    let detection = detect_host(repo_root, model).await?;
    let manifest_path = repo_root.join("models").join(model).join("manifest.json");
    let mut checks = vec![
        DoctorCheck {
            name: "cargo".to_string(),
            ok: command_exists("cargo"),
            required: true,
            detail: if command_exists("cargo") {
                "cargo is on PATH".to_string()
            } else {
                "cargo is required because this CLI launches workspace binaries".to_string()
            },
        },
        DoctorCheck {
            name: "model manifest".to_string(),
            ok: manifest_path.exists(),
            required: true,
            detail: if manifest_path.exists() {
                format!("found {}", manifest_path.display())
            } else {
                format!("missing {}", manifest_path.display())
            },
        },
        DoctorCheck {
            name: "control plane config".to_string(),
            ok: repo_root.join("config/dev/control-plane.toml").exists(),
            required: true,
            detail: config_detail(repo_root.join("config/dev/control-plane.toml")),
        },
        DoctorCheck {
            name: "gateway config".to_string(),
            ok: repo_root.join("config/dev/gateway.toml").exists(),
            required: true,
            detail: config_detail(repo_root.join("config/dev/gateway.toml")),
        },
        DoctorCheck {
            name: "node config".to_string(),
            ok: PathBuf::from(&detection.recommended_node_config).exists(),
            required: true,
            detail: config_detail(PathBuf::from(&detection.recommended_node_config)),
        },
    ];

    if let Some(runtime) = detection.recommended_runtime.as_deref() {
        match runtime {
            "mlx" => {
                let python = preferred_mlx_python(repo_root);
                checks.push(DoctorCheck {
                    name: "mlx python".to_string(),
                    ok: python.is_some(),
                    required: true,
                    detail: python
                        .as_ref()
                        .map(|path| format!("using {}", path.display()))
                        .unwrap_or_else(|| {
                            "expected .venv-mlx314, .venv-mlx, or python3 on PATH".to_string()
                        }),
                });
                let mlx_import_ok = python
                    .as_ref()
                    .map(|python| python_has_module(python, "mlx_lm"))
                    .transpose()?
                    .unwrap_or(false);
                checks.push(DoctorCheck {
                    name: "mlx_lm".to_string(),
                    ok: mlx_import_ok,
                    required: true,
                    detail: if mlx_import_ok {
                        "mlx_lm module was found".to_string()
                    } else {
                        "install mlx-lm into the selected Python environment".to_string()
                    },
                });
            }
            "llama.cpp" | "llamacpp" => {
                let llama_ok =
                    installed_llama_server(repo_root).is_some() || command_exists("llama-server");
                checks.push(DoctorCheck {
                    name: "llama-server".to_string(),
                    ok: llama_ok,
                    required: true,
                    detail: if llama_ok {
                        installed_llama_server(repo_root)
                            .map(|path| format!("found {}", path.display()))
                            .unwrap_or_else(|| "llama-server is on PATH".to_string())
                    } else {
                        "install a llama.cpp build that exposes llama-server(.exe)".to_string()
                    },
                });
                let needs_nvidia = detection
                    .accelerators
                    .iter()
                    .any(|accelerator| accelerator.eq_ignore_ascii_case("cuda"));
                if needs_nvidia {
                    let nvidia_ok = command_exists("nvidia-smi");
                    checks.push(DoctorCheck {
                        name: "nvidia-smi".to_string(),
                        ok: nvidia_ok,
                        required: true,
                        detail: if nvidia_ok {
                            "NVIDIA tooling is visible on PATH".to_string()
                        } else {
                            "install NVIDIA drivers so nvidia-smi is available".to_string()
                        },
                    });
                }
            }
            "vllm" => {
                let vllm_ok = command_exists("vllm");
                checks.push(DoctorCheck {
                    name: "vllm".to_string(),
                    ok: vllm_ok,
                    required: true,
                    detail: if vllm_ok {
                        "vllm is on PATH".to_string()
                    } else {
                        "install vllm for Linux CUDA deployments".to_string()
                    },
                });
            }
            "onnxruntime" | "onnx" => {
                checks.push(DoctorCheck {
                    name: "onnxruntime".to_string(),
                    ok: true,
                    required: false,
                    detail: "no explicit binary check is implemented yet".to_string(),
                });
            }
            _ => {}
        }
    }

    let blocking_checks = checks
        .iter()
        .filter(|check| check.required && !check.ok)
        .map(|check| check.name.clone())
        .collect::<Vec<_>>();
    Ok(DoctorReport {
        detection,
        ready: blocking_checks.is_empty(),
        checks,
        blocking_checks,
    })
}

fn runtime_env_overrides(repo_root: &Path, detection: &DetectionReport) -> Vec<(String, String)> {
    let mut overrides = Vec::new();
    if detection
        .recommended_runtime
        .as_deref()
        .is_some_and(|runtime| matches!(runtime, "llama.cpp" | "llamacpp"))
        && let Some(llama_server) = installed_llama_server(repo_root)
        && let Some(bin_dir) = llama_server.parent()
        && let Some(existing_path) = env::var_os("PATH")
    {
        let mut paths = vec![bin_dir.to_path_buf()];
        paths.extend(env::split_paths(&existing_path));
        if let Ok(joined) = env::join_paths(paths) {
            overrides.push(("PATH".to_string(), joined.to_string_lossy().into_owned()));
        }
    }
    overrides
}

fn installed_llama_server(repo_root: &Path) -> Option<PathBuf> {
    let install_root = repo_root.join("var/dev/runtimes/llama.cpp/current");
    if !install_root.exists() {
        return None;
    }
    find_file_recursive(&install_root, &executable_names("llama-server"))
}

fn find_file_recursive(root: &Path, names: &[String]) -> Option<PathBuf> {
    if !root.exists() {
        return None;
    }
    let mut stack = vec![root.to_path_buf()];
    while let Some(path) = stack.pop() {
        let entries = fs::read_dir(&path).ok()?;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            if let Some(name) = path.file_name().and_then(|name| name.to_str())
                && names
                    .iter()
                    .any(|candidate| candidate.eq_ignore_ascii_case(name))
            {
                return Some(path);
            }
        }
    }
    None
}

fn install_mlx_runtime(
    repo_root: &Path,
    dry_run: bool,
    _force: bool,
    actions: &mut Vec<InstallAction>,
) -> Result<()> {
    let bootstrap = command_path("python3.14")
        .or_else(|| command_path("python3"))
        .or_else(|| command_path("python"))
        .ok_or_else(|| {
            anyhow::anyhow!("python3.14, python3, or python is required to bootstrap mlx-lm")
        })?;
    let use_314_name = bootstrap
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.contains("3.14"));
    let venv_dir = if use_314_name {
        repo_root.join(".venv-mlx314")
    } else {
        repo_root.join(".venv-mlx")
    };
    let venv_python = venv_python_path(&venv_dir);

    if dry_run {
        actions.push(InstallAction {
            name: "create MLX venv".to_string(),
            status: "planned".to_string(),
            detail: format!("{} -m venv {}", bootstrap.display(), venv_dir.display()),
        });
        actions.push(InstallAction {
            name: "install mlx-lm".to_string(),
            status: "planned".to_string(),
            detail: format!(
                "{} -m pip install --upgrade pip mlx-lm",
                venv_python.display()
            ),
        });
        return Ok(());
    }

    if !venv_dir.exists() {
        let status = Command::new(&bootstrap)
            .args(["-m", "venv"])
            .arg(&venv_dir)
            .status()
            .with_context(|| format!("failed to create venv with {}", bootstrap.display()))?;
        if !status.success() {
            bail!("failed to create MLX venv at {}", venv_dir.display());
        }
        actions.push(InstallAction {
            name: "create MLX venv".to_string(),
            status: "ok".to_string(),
            detail: format!("created {}", venv_dir.display()),
        });
    } else {
        actions.push(InstallAction {
            name: "create MLX venv".to_string(),
            status: "skipped".to_string(),
            detail: format!("reusing {}", venv_dir.display()),
        });
    }

    run_command_checked(
        &venv_python,
        [
            String::from("-m"),
            String::from("pip"),
            String::from("install"),
            String::from("--upgrade"),
            String::from("pip"),
        ],
        repo_root,
    )?;
    run_command_checked(
        &venv_python,
        [
            String::from("-m"),
            String::from("pip"),
            String::from("install"),
            String::from("--upgrade"),
            String::from("mlx-lm"),
        ],
        repo_root,
    )?;
    actions.push(InstallAction {
        name: "install mlx-lm".to_string(),
        status: "ok".to_string(),
        detail: format!("installed into {}", venv_dir.display()),
    });
    Ok(())
}

async fn install_llama_cpp_runtime(
    repo_root: &Path,
    detection: &DetectionReport,
    dry_run: bool,
    force: bool,
    actions: &mut Vec<InstallAction>,
) -> Result<()> {
    let install_root = repo_root.join("var/dev/runtimes/llama.cpp/current");
    if install_root.exists() && !force && installed_llama_server(repo_root).is_some() {
        actions.push(InstallAction {
            name: "install llama.cpp".to_string(),
            status: "skipped".to_string(),
            detail: format!("reusing {}", install_root.display()),
        });
        return Ok(());
    }

    let client = github_client()?;
    let release = fetch_latest_llama_cpp_release(&client).await?;
    let assets = select_llama_cpp_assets(detection, &release)?;
    let downloads_dir = repo_root.join("var/dev/downloads/llama.cpp");

    if dry_run {
        actions.push(InstallAction {
            name: "download llama.cpp".to_string(),
            status: "planned".to_string(),
            detail: format!(
                "release {} assets: {}",
                release.tag_name,
                assets
                    .iter()
                    .map(|asset| asset.name.clone())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        });
        actions.push(InstallAction {
            name: "extract llama.cpp".to_string(),
            status: "planned".to_string(),
            detail: format!("extract into {}", install_root.display()),
        });
        return Ok(());
    }

    fs::create_dir_all(&downloads_dir)
        .with_context(|| format!("failed to create {}", downloads_dir.display()))?;
    if force && install_root.exists() {
        fs::remove_dir_all(&install_root)
            .with_context(|| format!("failed to reset {}", install_root.display()))?;
    }
    fs::create_dir_all(&install_root)
        .with_context(|| format!("failed to create {}", install_root.display()))?;

    let mut downloaded = Vec::new();
    for asset in &assets {
        let destination = downloads_dir.join(&asset.name);
        download_to_path(&client, &asset.browser_download_url, &destination).await?;
        extract_archive(&destination, &install_root)?;
        downloaded.push(asset.name.clone());
    }
    let server = installed_llama_server(repo_root)
        .ok_or_else(|| anyhow::anyhow!("llama-server was not found after extraction"))?;
    fs::write(
        install_root.join("install.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "release": release.tag_name,
            "assets": downloaded,
            "llama_server": server.display().to_string(),
        }))?,
    )
    .with_context(|| {
        format!(
            "failed to write {}",
            install_root.join("install.json").display()
        )
    })?;

    actions.push(InstallAction {
        name: "install llama.cpp".to_string(),
        status: "ok".to_string(),
        detail: format!(
            "installed {} into {}",
            release.tag_name,
            install_root.display()
        ),
    });
    Ok(())
}

fn venv_python_path(venv_dir: &Path) -> PathBuf {
    #[cfg(windows)]
    {
        venv_dir.join("Scripts/python.exe")
    }

    #[cfg(not(windows))]
    {
        venv_dir.join("bin/python3")
    }
}

fn run_command_checked<I, S>(program: &Path, args: I, cwd: &Path) -> Result<()>
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    let status = Command::new(program)
        .current_dir(cwd)
        .args(args)
        .status()
        .with_context(|| format!("failed to launch {}", program.display()))?;
    if !status.success() {
        bail!("command failed: {}", program.display());
    }
    Ok(())
}

fn github_client() -> Result<reqwest::Client> {
    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, HeaderValue::from_static("galactica-cli"));
    headers.insert(
        ACCEPT,
        HeaderValue::from_static("application/vnd.github+json"),
    );
    if let Ok(token) = env::var("GITHUB_TOKEN")
        && !token.trim().is_empty()
    {
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}"))
                .context("failed to encode GITHUB_TOKEN")?,
        );
    }
    reqwest::Client::builder()
        .default_headers(headers)
        .build()
        .context("failed to initialize HTTP client")
}

async fn fetch_latest_llama_cpp_release(client: &reqwest::Client) -> Result<GithubRelease> {
    let url = env::var("GALACTICA_LLAMA_CPP_RELEASE_API_URL").unwrap_or_else(|_| {
        "https://api.github.com/repos/ggml-org/llama.cpp/releases/latest".to_string()
    });
    fetch_latest_release(client, &url).await
}

async fn fetch_latest_release(client: &reqwest::Client, url: &str) -> Result<GithubRelease> {
    client
        .get(url)
        .send()
        .await
        .context("failed to query GitHub release API")?
        .error_for_status()
        .context("GitHub release API returned an error")?
        .json::<GithubRelease>()
        .await
        .context("failed to parse GitHub release metadata")
}

fn select_llama_cpp_assets(
    detection: &DetectionReport,
    release: &GithubRelease,
) -> Result<Vec<GithubAsset>> {
    let os = detection.os.as_str();
    let has_cuda = detection
        .accelerators
        .iter()
        .any(|accelerator| accelerator.eq_ignore_ascii_case("cuda"));
    let has_rocm = detection
        .accelerators
        .iter()
        .any(|accelerator| accelerator.eq_ignore_ascii_case("rocm"));

    let mut selected = Vec::new();
    if os == "windows" && has_cuda {
        if let Some(asset) = find_asset(release, &["bin-win-cuda-12.4-x64"], &[])
            .or_else(|| find_asset(release, &["bin-win-cuda-13.1-x64"], &[]))
            .or_else(|| find_asset(release, &["bin-win-cuda", "x64"], &[]))
        {
            selected.push(asset);
        }
        if let Some(asset) = find_asset(release, &["cudart-llama-bin-win-cuda-12.4-x64"], &[])
            .or_else(|| find_asset(release, &["cudart-llama-bin-win-cuda-13.1-x64"], &[]))
        {
            selected.push(asset);
        }
    } else if os == "windows" {
        if let Some(asset) = find_asset(release, &["bin-win-cpu-x64"], &[])
            .or_else(|| find_asset(release, &["bin-win", "cpu", "x64"], &[]))
        {
            selected.push(asset);
        }
    } else if os == "linux" && has_rocm {
        if let Some(asset) = find_asset(release, &["bin-ubuntu-rocm", "x64"], &[]) {
            selected.push(asset);
        }
    } else if os == "linux"
        && let Some(asset) = find_asset(release, &["bin-ubuntu", "x64"], &["vulkan", "rocm"])
    {
        selected.push(asset);
    } else if os == "macos" && detection.cpu_arch == "aarch64" {
        if let Some(asset) = find_asset(release, &["bin-macos-arm64"], &[])
            .or_else(|| find_asset(release, &["bin-macos", "arm64"], &["x64"]))
        {
            selected.push(asset);
        }
    } else if os == "macos"
        && detection.cpu_arch == "x86_64"
        && let Some(asset) = find_asset(release, &["bin-macos-x64"], &[])
            .or_else(|| find_asset(release, &["bin-macos", "x64"], &["arm64"]))
    {
        selected.push(asset);
    }

    if selected.is_empty() {
        bail!(
            "could not find a matching llama.cpp release asset for {} {} in {}",
            detection.os,
            join_or_na(&detection.accelerators),
            release.tag_name
        );
    }
    Ok(selected)
}

fn find_asset(release: &GithubRelease, all: &[&str], none: &[&str]) -> Option<GithubAsset> {
    release.assets.iter().find_map(|asset| {
        let name = asset.name.to_ascii_lowercase();
        let all_match = all
            .iter()
            .all(|pattern| name.contains(&pattern.to_ascii_lowercase()));
        let none_match = none
            .iter()
            .any(|pattern| name.contains(&pattern.to_ascii_lowercase()));
        if all_match && !none_match {
            Some(asset.clone())
        } else {
            None
        }
    })
}

async fn download_to_path(client: &reqwest::Client, url: &str, destination: &Path) -> Result<()> {
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("failed to download {url}"))?
        .error_for_status()
        .with_context(|| format!("download failed for {url}"))?;
    let mut stream = response.bytes_stream();
    let mut file = tokio::fs::File::create(destination)
        .await
        .with_context(|| format!("failed to create {}", destination.display()))?;
    use futures::StreamExt;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.with_context(|| format!("failed while downloading {url}"))?;
        file.write_all(&chunk)
            .await
            .with_context(|| format!("failed to write {}", destination.display()))?;
    }
    file.flush()
        .await
        .with_context(|| format!("failed to flush {}", destination.display()))?;
    Ok(())
}

fn extract_archive(archive_path: &Path, destination: &Path) -> Result<()> {
    let filename = archive_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    if filename.ends_with(".zip") {
        extract_zip(archive_path, destination)
    } else if filename.ends_with(".tar.gz") || filename.ends_with(".tgz") {
        extract_tar_gz(archive_path, destination)
    } else {
        bail!("unsupported archive format: {}", archive_path.display());
    }
}

fn extract_zip(archive_path: &Path, destination: &Path) -> Result<()> {
    let file = File::open(archive_path)
        .with_context(|| format!("failed to open {}", archive_path.display()))?;
    let mut archive = ZipArchive::new(file)
        .with_context(|| format!("failed to read zip archive {}", archive_path.display()))?;
    for index in 0..archive.len() {
        let mut entry = archive.by_index(index).with_context(|| {
            format!(
                "failed to read archive entry {index} in {}",
                archive_path.display()
            )
        })?;
        let Some(relative_path) = entry.enclosed_name() else {
            continue;
        };
        let out_path = destination.join(relative_path);
        if entry.is_dir() {
            fs::create_dir_all(&out_path)
                .with_context(|| format!("failed to create {}", out_path.display()))?;
            continue;
        }
        if let Some(parent) = out_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        let mut out_file = File::create(&out_path)
            .with_context(|| format!("failed to create {}", out_path.display()))?;
        io::copy(&mut entry, &mut out_file)
            .with_context(|| format!("failed to extract {}", out_path.display()))?;
        #[cfg(unix)]
        if let Some(mode) = entry.unix_mode() {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&out_path, fs::Permissions::from_mode(mode))
                .with_context(|| format!("failed to set permissions on {}", out_path.display()))?;
        }
    }
    Ok(())
}

fn extract_tar_gz(archive_path: &Path, destination: &Path) -> Result<()> {
    let file = File::open(archive_path)
        .with_context(|| format!("failed to open {}", archive_path.display()))?;
    let decoder = flate2::read::GzDecoder::new(file);
    let mut archive = tar::Archive::new(decoder);
    archive
        .unpack(destination)
        .with_context(|| format!("failed to unpack {}", archive_path.display()))
}

fn mint_enrollment_token(repo_root: &Path, ttl_seconds: Option<u64>) -> Result<String> {
    fs::create_dir_all(repo_root.join("var/dev"))
        .with_context(|| format!("failed to create {}", repo_root.join("var/dev").display()))?;
    let mut args = vec![
        "run".to_string(),
        "-q".to_string(),
        "-p".to_string(),
        "galactica-control-plane".to_string(),
        "--".to_string(),
        "--config".to_string(),
        repo_root
            .join("config/dev/control-plane.toml")
            .display()
            .to_string(),
        "--mint-enrollment-token-only".to_string(),
    ];
    if let Some(ttl_seconds) = ttl_seconds {
        args.push("--enrollment-token-ttl-seconds".to_string());
        args.push(ttl_seconds.to_string());
    }
    let output = Command::new("cargo")
        .current_dir(repo_root)
        .args(args)
        .output()
        .context("failed to launch cargo for enrollment token minting")?;
    if !output.status.success() {
        bail!(
            "failed to mint enrollment token:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let token = String::from_utf8(output.stdout)
        .context("control plane printed invalid UTF-8")?
        .trim()
        .to_string();
    if token.is_empty() {
        bail!("control plane did not print an enrollment token");
    }
    let token_file = repo_root.join("var/dev/enrollment-token.txt");
    fs::write(&token_file, format!("{token}\n"))
        .with_context(|| format!("failed to write {}", token_file.display()))?;
    Ok(token)
}

fn default_install_root() -> Result<PathBuf> {
    #[cfg(windows)]
    {
        if let Ok(local_app_data) = env::var("LOCALAPPDATA")
            && !local_app_data.trim().is_empty()
        {
            return Ok(PathBuf::from(local_app_data).join("Programs/Galactica"));
        }
        if let Ok(user_profile) = env::var("USERPROFILE")
            && !user_profile.trim().is_empty()
        {
            return Ok(PathBuf::from(user_profile).join("AppData/Local/Programs/Galactica"));
        }
    }

    #[cfg(not(windows))]
    {
        if let Ok(home) = env::var("HOME")
            && !home.trim().is_empty()
        {
            return Ok(PathBuf::from(home).join(".local"));
        }
    }

    bail!("could not determine a default user install root")
}

fn installed_binary_name() -> &'static str {
    #[cfg(windows)]
    {
        "galactica-cli.exe"
    }

    #[cfg(not(windows))]
    {
        "galactica-cli"
    }
}

fn launcher_path(bin_dir: &Path) -> PathBuf {
    #[cfg(windows)]
    {
        bin_dir.join("galactica.cmd")
    }

    #[cfg(not(windows))]
    {
        bin_dir.join("galactica")
    }
}

fn cargo_install_args(repo_root: &Path, install_root: &Path, force: bool) -> Vec<String> {
    let mut args = vec![
        "install".to_string(),
        "--path".to_string(),
        repo_root.join("rust/galactica-cli").display().to_string(),
        "--root".to_string(),
        install_root.display().to_string(),
    ];
    if force {
        args.push("--force".to_string());
    }
    args
}

async fn install_cli_from_release_asset(
    repo_root: &Path,
    bin_dir: &Path,
    binary_path: &Path,
    force: bool,
) -> Result<String> {
    let client = github_client()?;
    let api_url = galactica_cli_release_api_url(repo_root)?;
    let release = fetch_latest_release(&client, &api_url).await?;
    let workspace_version = workspace_version(repo_root)?;
    let release_version = normalize_release_tag(&release.tag_name);
    if release_version != workspace_version {
        bail!(
            "latest release {} does not match workspace version {}; using cargo install",
            release.tag_name,
            workspace_version
        );
    }
    let asset = select_galactica_cli_asset_for_host(&release)?;
    let downloads_dir = repo_root.join("var/dev/downloads/galactica-cli");
    fs::create_dir_all(&downloads_dir)
        .with_context(|| format!("failed to create {}", downloads_dir.display()))?;
    let archive_path = downloads_dir.join(&asset.name);
    if force || !archive_path.exists() {
        download_to_path(&client, &asset.browser_download_url, &archive_path).await?;
    }
    install_binary_from_archive(&archive_path, bin_dir, binary_path)?;
    Ok(format!(
        "installed {} from {}",
        asset.name, release.tag_name
    ))
}

fn run_cargo_install(repo_root: &Path, cargo_install_args: &[String]) -> Result<()> {
    let status = Command::new("cargo")
        .current_dir(repo_root)
        .args(cargo_install_args)
        .status()
        .context("failed to run cargo install")?;
    if !status.success() {
        bail!("cargo install failed with status {status}");
    }
    Ok(())
}

fn galactica_cli_release_api_url(repo_root: &Path) -> Result<String> {
    if let Ok(url) = env::var("GALACTICA_CLI_RELEASE_API_URL")
        && !url.trim().is_empty()
    {
        return Ok(url);
    }
    let repository_url = workspace_repository_url(repo_root)?;
    let slug = github_repo_slug_from_url(&repository_url).ok_or_else(|| {
        anyhow::anyhow!(
            "failed to derive a GitHub repository slug from {}",
            repository_url
        )
    })?;
    Ok(format!(
        "https://api.github.com/repos/{slug}/releases/latest"
    ))
}

fn workspace_repository_url(repo_root: &Path) -> Result<String> {
    let cargo_toml_path = repo_root.join("Cargo.toml");
    let contents = fs::read_to_string(&cargo_toml_path)
        .with_context(|| format!("failed to read {}", cargo_toml_path.display()))?;
    extract_workspace_string_value(&contents, "repository").ok_or_else(|| {
        anyhow::anyhow!(
            "failed to find workspace.package.repository in {}",
            cargo_toml_path.display()
        )
    })
}

fn workspace_version(repo_root: &Path) -> Result<String> {
    let cargo_toml_path = repo_root.join("Cargo.toml");
    let contents = fs::read_to_string(&cargo_toml_path)
        .with_context(|| format!("failed to read {}", cargo_toml_path.display()))?;
    extract_workspace_string_value(&contents, "version").ok_or_else(|| {
        anyhow::anyhow!(
            "failed to find workspace.package.version in {}",
            cargo_toml_path.display()
        )
    })
}

fn extract_workspace_string_value(cargo_toml: &str, key: &str) -> Option<String> {
    let mut in_workspace_package = false;
    for raw_line in cargo_toml.lines() {
        let line = raw_line.split('#').next()?.trim();
        if line.is_empty() {
            continue;
        }
        if line.starts_with('[') {
            in_workspace_package = line == "[workspace.package]";
            continue;
        }
        if !in_workspace_package {
            continue;
        }
        let prefix = format!("{key} = ");
        if !line.starts_with(&prefix) {
            continue;
        }
        let value = line[prefix.len()..].trim();
        return value
            .strip_prefix('"')
            .and_then(|value| value.strip_suffix('"'))
            .map(ToString::to_string);
    }
    None
}

fn github_repo_slug_from_url(url: &str) -> Option<String> {
    let trimmed = url.trim().trim_end_matches('/');
    if let Some(rest) = trimmed.strip_prefix("https://github.com/") {
        return Some(rest.trim_end_matches(".git").to_string());
    }
    if let Some(rest) = trimmed.strip_prefix("http://github.com/") {
        return Some(rest.trim_end_matches(".git").to_string());
    }
    if let Some(rest) = trimmed.strip_prefix("git@github.com:") {
        return Some(rest.trim_end_matches(".git").to_string());
    }
    None
}

fn write_repo_launcher(repo_root: &Path, binary_path: &Path, launcher_path: &Path) -> Result<()> {
    let contents = launcher_contents(repo_root, binary_path);
    if let Some(parent) = launcher_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    fs::write(launcher_path, contents)
        .with_context(|| format!("failed to write {}", launcher_path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(launcher_path, fs::Permissions::from_mode(0o755))
            .with_context(|| format!("failed to set permissions on {}", launcher_path.display()))?;
    }
    Ok(())
}

fn launcher_contents(repo_root: &Path, binary_path: &Path) -> String {
    #[cfg(windows)]
    {
        format!(
            "@echo off\r\n\"{}\" --repo-root \"{}\" %*\r\n",
            binary_path.display(),
            repo_root.display()
        )
    }

    #[cfg(not(windows))]
    {
        format!(
            "#!/usr/bin/env sh\nexec \"{}\" --repo-root \"{}\" \"$@\"\n",
            binary_path.display(),
            repo_root.display()
        )
    }
}

fn path_contains_dir(dir: &Path) -> bool {
    let dir = normalize_path(dir);
    env::var_os("PATH").is_some_and(|paths| {
        env::split_paths(&paths)
            .map(|candidate| normalize_path(&candidate))
            .any(|candidate| candidate == dir)
    })
}

fn normalize_path(path: &Path) -> String {
    #[cfg(windows)]
    {
        path.to_string_lossy()
            .replace('\\', "/")
            .to_ascii_lowercase()
    }

    #[cfg(not(windows))]
    {
        path.to_string_lossy().into_owned()
    }
}

fn path_hint(bin_dir: &Path) -> String {
    #[cfg(windows)]
    {
        format!("setx PATH \"%PATH%;{}\"", bin_dir.display())
    }

    #[cfg(not(windows))]
    {
        let shell = env::var("SHELL").unwrap_or_default();
        let profile = if shell.contains("zsh") {
            "~/.zshrc"
        } else if shell.contains("bash") {
            "~/.bashrc"
        } else {
            "~/.profile"
        };
        format!(
            "echo 'export PATH=\"{}:$PATH\"' >> {}",
            bin_dir.display(),
            profile
        )
    }
}

fn galactica_cli_target_triple(os: &str, arch: &str) -> Option<&'static str> {
    match (os, arch) {
        ("macos", "aarch64") => Some("aarch64-apple-darwin"),
        ("macos", "x86_64") => Some("x86_64-apple-darwin"),
        ("windows", "x86_64") => Some("x86_64-pc-windows-msvc"),
        ("linux", "x86_64") => Some("x86_64-unknown-linux-gnu"),
        _ => None,
    }
}

fn release_archive_suffix_for_os(os: &str) -> Option<&'static str> {
    match os {
        "windows" => Some(".zip"),
        "macos" | "linux" => Some(".tar.gz"),
        _ => None,
    }
}

fn normalize_release_tag(tag: &str) -> &str {
    tag.strip_prefix('v')
        .or_else(|| tag.strip_prefix('V'))
        .unwrap_or(tag)
}

fn select_galactica_cli_asset_for_host(release: &GithubRelease) -> Result<GithubAsset> {
    let target =
        galactica_cli_target_triple(env::consts::OS, env::consts::ARCH).ok_or_else(|| {
            anyhow::anyhow!(
                "no galactica-cli release asset mapping for {} {}",
                env::consts::OS,
                env::consts::ARCH
            )
        })?;
    let suffix = release_archive_suffix_for_os(env::consts::OS).ok_or_else(|| {
        anyhow::anyhow!("no release archive format mapping for {}", env::consts::OS)
    })?;
    select_galactica_cli_asset(release, target, suffix)
}

fn select_galactica_cli_asset(
    release: &GithubRelease,
    target: &str,
    archive_suffix: &str,
) -> Result<GithubAsset> {
    release
        .assets
        .iter()
        .find(|asset| {
            let name = asset.name.to_ascii_lowercase();
            name.contains("galactica-cli")
                && name.contains(&target.to_ascii_lowercase())
                && name.ends_with(&archive_suffix.to_ascii_lowercase())
        })
        .cloned()
        .ok_or_else(|| {
            anyhow::anyhow!(
                "could not find a galactica-cli asset for target {} in {}",
                target,
                release.tag_name
            )
        })
}

fn install_binary_from_archive(
    archive_path: &Path,
    bin_dir: &Path,
    binary_path: &Path,
) -> Result<()> {
    let temp_dir = temp_extract_dir("galactica-cli-self-install");
    if temp_dir.exists() {
        fs::remove_dir_all(&temp_dir)
            .with_context(|| format!("failed to reset {}", temp_dir.display()))?;
    }
    fs::create_dir_all(&temp_dir)
        .with_context(|| format!("failed to create {}", temp_dir.display()))?;
    extract_archive(archive_path, &temp_dir)?;
    let extracted_binary = find_file_recursive(&temp_dir, &executable_names("galactica-cli"))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "galactica-cli binary was not found in {}",
                archive_path.display()
            )
        })?;
    fs::create_dir_all(bin_dir)
        .with_context(|| format!("failed to create {}", bin_dir.display()))?;
    copy_executable(&extracted_binary, binary_path)?;
    fs::remove_dir_all(&temp_dir)
        .with_context(|| format!("failed to clean up {}", temp_dir.display()))?;
    Ok(())
}

fn temp_extract_dir(prefix: &str) -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    env::temp_dir().join(format!("{prefix}-{}-{stamp}", std::process::id()))
}

fn copy_executable(source: &Path, destination: &Path) -> Result<()> {
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    fs::copy(source, destination).with_context(|| {
        format!(
            "failed to copy {} to {}",
            source.display(),
            destination.display()
        )
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(destination, fs::Permissions::from_mode(0o755))
            .with_context(|| format!("failed to set permissions on {}", destination.display()))?;
    }
    Ok(())
}

fn resolve_enrollment_token(repo_root: &Path, token: Option<String>) -> Result<String> {
    if let Some(token) = token.filter(|value| !value.trim().is_empty()) {
        return Ok(token);
    }
    if let Ok(token) = env::var("GALACTICA_ENROLLMENT_TOKEN")
        && !token.trim().is_empty()
    {
        return Ok(token);
    }
    let token_file = repo_root.join("var/dev/enrollment-token.txt");
    if token_file.exists() {
        let token = fs::read_to_string(&token_file)
            .with_context(|| format!("failed to read {}", token_file.display()))?;
        let token = token.trim().to_string();
        if !token.is_empty() {
            return Ok(token);
        }
    }
    bail!("missing enrollment token; run `galactica-cli token mint` first or pass --token")
}

fn run_workspace_command(
    repo_root: &Path,
    package: &str,
    service_args: Vec<String>,
    dry_run: bool,
    quiet: bool,
    env_overrides: &[(String, String)],
) -> Result<()> {
    let mut args = vec!["run".to_string()];
    if quiet {
        args.push("-q".to_string());
    }
    args.extend(["-p".to_string(), package.to_string(), "--".to_string()]);
    args.extend(service_args);

    if dry_run {
        for (key, value) in env_overrides {
            println!("{key}={value}");
        }
        println!("cargo {}", shell_join(&args));
        return Ok(());
    }

    let mut command = Command::new("cargo");
    command.current_dir(repo_root).args(&args);
    for (key, value) in env_overrides {
        command.env(key, value);
    }
    let status = command
        .status()
        .with_context(|| format!("failed to launch cargo for {package}"))?;
    if !status.success() {
        bail!("{package} exited with status {status}");
    }
    Ok(())
}

fn next_steps(detection: &DetectionReport) -> Vec<String> {
    let mut steps = Vec::new();
    match detection.os.as_str() {
        "macos" => {
            steps.push("galactica-cli up control-plane".to_string());
            steps.push("galactica-cli up gateway".to_string());
            steps.push("galactica-cli token mint".to_string());
            steps.push("galactica-cli up node".to_string());
        }
        "windows" => {
            steps.push("galactica-cli doctor".to_string());
            steps.push(
                "galactica-cli up node --token <TOKEN> --control-plane-addr http://<MAC_IP>:9090"
                    .to_string(),
            );
        }
        _ => {
            steps.push("galactica-cli doctor".to_string());
            steps.push("galactica-cli up node --token <TOKEN>".to_string());
        }
    }
    steps
}

fn build_notes(
    model: &str,
    manifest: Option<&ModelManifest>,
    capabilities: &NodeCapabilities,
    variant: Option<&galactica_common::proto::common::v1::ModelVariant>,
) -> Vec<String> {
    let mut notes = Vec::new();
    let os = OsType::try_from(capabilities.os).unwrap_or_default();
    let cpu_arch = CpuArch::try_from(capabilities.cpu_arch).unwrap_or_default();
    if manifest.is_none() {
        notes.push(format!(
            "model manifest for `{model}` was not found under {}",
            PathBuf::from("models")
                .join(model)
                .join("manifest.json")
                .display()
        ));
    }
    if variant.is_none() {
        notes.push("no compatible model variant matched this machine; falling back to runtime-only recommendation".to_string());
    }
    if os == OsType::Macos && cpu_arch == CpuArch::X8664 {
        notes.push(
            "Intel macOS uses llama.cpp; MLX/Metal acceleration is Apple Silicon-only".to_string(),
        );
    }
    if capabilities.runtime_backends.is_empty() {
        notes.push("the hardware detector did not find any runtime backends on PATH".to_string());
    }
    notes
}

fn preferred_mlx_python(repo_root: &Path) -> Option<PathBuf> {
    let candidates = [
        repo_root.join(".venv-mlx314/bin/python3.14"),
        repo_root.join(".venv-mlx314/bin/python3"),
        repo_root.join(".venv-mlx/bin/python3"),
        repo_root.join(".venv-mlx314/Scripts/python.exe"),
        repo_root.join(".venv-mlx/Scripts/python.exe"),
    ];
    candidates
        .into_iter()
        .find(|candidate| candidate.exists())
        .or_else(|| command_path("python3"))
        .or_else(|| command_path("python"))
}

fn python_has_module(python: &Path, module: &str) -> Result<bool> {
    let output = Command::new(python)
        .args([
            "-c",
            &format!(
                "import importlib.util, sys; sys.exit(0 if importlib.util.find_spec({module:?}) else 1)"
            ),
        ])
        .output()
        .with_context(|| format!("failed to launch {}", python.display()))?;
    Ok(output.status.success())
}

fn fallback_runtime(capabilities: &NodeCapabilities) -> Option<String> {
    capabilities
        .runtime_backends
        .iter()
        .max_by_key(|runtime| runtime_preference_score(runtime, capabilities))
        .cloned()
}

fn add_repo_runtime_backends(repo_root: &Path, capabilities: &mut NodeCapabilities) {
    if installed_llama_server(repo_root).is_some()
        && !capabilities
            .runtime_backends
            .iter()
            .any(|runtime| runtime.eq_ignore_ascii_case("llama.cpp"))
    {
        capabilities.runtime_backends.push("llama.cpp".to_string());
        capabilities.runtime_backends.sort();
    }
}

fn find_repo_root(override_root: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(root) = override_root {
        return Ok(root);
    }

    let mut candidates = Vec::new();
    candidates.push(env::current_dir().context("failed to resolve current directory")?);
    if let Ok(exe) = env::current_exe()
        && let Some(parent) = exe.parent()
    {
        candidates.push(parent.to_path_buf());
    }

    for candidate in candidates {
        for ancestor in candidate.ancestors() {
            let root = ancestor.to_path_buf();
            if root.join("Cargo.toml").exists() && root.join("models").exists() {
                return Ok(root);
            }
        }
    }

    bail!("failed to locate repository root; pass --repo-root explicitly")
}

fn command_exists(command: &str) -> bool {
    command_path(command).is_some()
}

fn command_path(command: &str) -> Option<PathBuf> {
    env::var_os("PATH").and_then(|paths| {
        env::split_paths(&paths).find_map(|path| {
            executable_names(command)
                .into_iter()
                .map(|name| path.join(name))
                .find(|candidate| candidate.exists())
        })
    })
}

fn executable_names(command: &str) -> Vec<String> {
    #[cfg(windows)]
    {
        if command.contains('.') {
            vec![command.to_string()]
        } else {
            vec![
                format!("{command}.exe"),
                format!("{command}.cmd"),
                format!("{command}.bat"),
            ]
        }
    }

    #[cfg(not(windows))]
    {
        vec![command.to_string()]
    }
}

fn detect_local_ip() -> Option<String> {
    let socket = UdpSocket::bind("0.0.0.0:0").ok()?;
    socket.connect("8.8.8.8:80").ok()?;
    let ip = socket.local_addr().ok()?.ip();
    match ip {
        IpAddr::V4(ip) if !ip.is_loopback() => Some(ip.to_string()),
        IpAddr::V6(ip) if !ip.is_loopback() => Some(ip.to_string()),
        _ => None,
    }
}

fn default_node_config_for_capabilities(capabilities: &NodeCapabilities) -> PathBuf {
    let os = OsType::try_from(capabilities.os).unwrap_or_default();
    match os {
        OsType::Macos => PathBuf::from("config/dev/macos-node-agent.toml"),
        OsType::Windows => PathBuf::from("config/dev/windows-node-agent.toml"),
        _ => PathBuf::from("config/dev/linux-node-agent.toml"),
    }
}

fn accelerator_labels(capabilities: &NodeCapabilities) -> Vec<String> {
    let mut labels = capabilities
        .accelerators
        .iter()
        .map(|accelerator| {
            match AcceleratorType::try_from(accelerator.r#type).unwrap_or_default() {
                AcceleratorType::Metal => "metal",
                AcceleratorType::Cuda => "cuda",
                AcceleratorType::Rocm => "rocm",
                AcceleratorType::Directml => "directml",
                AcceleratorType::Cpu => "cpu",
                AcceleratorType::Unspecified => "unspecified",
            }
            .to_string()
        })
        .collect::<Vec<_>>();
    labels.sort();
    labels.dedup();
    labels
}

fn pool_label(capabilities: &NodeCapabilities) -> String {
    format!(
        "{}-{}-{}",
        os_label(capabilities.os),
        accelerator_label(capabilities),
        arch_label(capabilities.cpu_arch)
    )
}

fn os_label(value: i32) -> &'static str {
    match OsType::try_from(value).unwrap_or_default() {
        OsType::Macos => "macos",
        OsType::Linux => "linux",
        OsType::Windows => "windows",
        OsType::Unspecified => "unknown",
    }
}

fn accelerator_label(capabilities: &NodeCapabilities) -> &'static str {
    let accelerator = capabilities
        .accelerators
        .iter()
        .find(|accelerator| accelerator.r#type != AcceleratorType::Cpu as i32)
        .or_else(|| capabilities.accelerators.first());
    match accelerator
        .map(|accelerator| AcceleratorType::try_from(accelerator.r#type).unwrap_or_default())
        .unwrap_or(AcceleratorType::Cpu)
    {
        AcceleratorType::Metal => "metal",
        AcceleratorType::Cuda => "cuda",
        AcceleratorType::Rocm => "rocm",
        AcceleratorType::Directml => "directml",
        AcceleratorType::Cpu | AcceleratorType::Unspecified => "cpu",
    }
}

fn arch_label(value: i32) -> &'static str {
    match CpuArch::try_from(value).unwrap_or_default() {
        CpuArch::Arm64 => "arm64",
        CpuArch::X8664 => "x86_64",
        CpuArch::Unspecified => "unknown",
    }
}

fn config_detail(path: PathBuf) -> String {
    if path.exists() {
        format!("found {}", path.display())
    } else {
        format!("missing {}", path.display())
    }
}

fn join_or_na(values: &[String]) -> String {
    if values.is_empty() {
        "n/a".to_string()
    } else {
        values.join(", ")
    }
}

fn shell_join(args: &[String]) -> String {
    args.iter()
        .map(|arg| {
            if arg.contains([' ', '\t', '"']) {
                format!("{arg:?}")
            } else {
                arg.clone()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::{
        DetectionReport, GithubAsset, GithubRelease, arch_label,
        default_node_config_for_capabilities, extract_workspace_string_value,
        galactica_cli_target_triple, github_repo_slug_from_url, normalize_release_tag, os_label,
        pool_label, select_galactica_cli_asset, select_llama_cpp_assets,
    };
    use galactica_common::proto::common::v1::{
        AcceleratorInfo, AcceleratorType, CpuArch, Memory, NetworkProfile, NodeCapabilities, OsType,
    };

    fn windows_cuda_capabilities() -> NodeCapabilities {
        NodeCapabilities {
            os: OsType::Windows as i32,
            cpu_arch: CpuArch::X8664 as i32,
            accelerators: vec![
                AcceleratorInfo {
                    r#type: AcceleratorType::Cuda as i32,
                    name: "RTX".to_string(),
                    vram: Some(Memory {
                        total_bytes: 16,
                        available_bytes: 16,
                    }),
                    compute_capability: "sm89".to_string(),
                },
                AcceleratorInfo {
                    r#type: AcceleratorType::Cpu as i32,
                    name: "CPU".to_string(),
                    vram: None,
                    compute_capability: String::new(),
                },
            ],
            system_memory: Some(Memory {
                total_bytes: 64,
                available_bytes: 48,
            }),
            network_profile: NetworkProfile::Lan as i32,
            runtime_backends: vec!["llama.cpp".to_string(), "onnxruntime".to_string()],
            locality: Default::default(),
        }
    }

    #[test]
    fn pool_labels_windows_cuda_nodes() {
        assert_eq!(
            pool_label(&windows_cuda_capabilities()),
            "windows-cuda-x86_64"
        );
    }

    #[test]
    fn defaults_to_windows_node_config_on_windows() {
        assert_eq!(
            default_node_config_for_capabilities(&windows_cuda_capabilities()),
            std::path::PathBuf::from("config/dev/windows-node-agent.toml")
        );
    }

    #[test]
    fn os_and_arch_labels_stay_stable() {
        assert_eq!(os_label(OsType::Macos as i32), "macos");
        assert_eq!(arch_label(CpuArch::Arm64 as i32), "arm64");
    }

    fn sample_release() -> GithubRelease {
        GithubRelease {
            tag_name: "b-test".to_string(),
            assets: vec![
                GithubAsset {
                    name: "llama-b-test-bin-win-cuda-12.4-x64.zip".to_string(),
                    browser_download_url: "https://example.com/win-cuda.zip".to_string(),
                },
                GithubAsset {
                    name: "cudart-llama-bin-win-cuda-12.4-x64.zip".to_string(),
                    browser_download_url: "https://example.com/cudart.zip".to_string(),
                },
                GithubAsset {
                    name: "llama-b-test-bin-ubuntu-x64.tar.gz".to_string(),
                    browser_download_url: "https://example.com/linux-cpu.tar.gz".to_string(),
                },
                GithubAsset {
                    name: "llama-b-test-bin-ubuntu-vulkan-x64.tar.gz".to_string(),
                    browser_download_url: "https://example.com/linux-vulkan.tar.gz".to_string(),
                },
                GithubAsset {
                    name: "llama-b-test-bin-macos-arm64.zip".to_string(),
                    browser_download_url: "https://example.com/macos-arm64.zip".to_string(),
                },
                GithubAsset {
                    name: "llama-b-test-bin-macos-x64.zip".to_string(),
                    browser_download_url: "https://example.com/macos-x64.zip".to_string(),
                },
                GithubAsset {
                    name: "galactica-cli-v0.1.0-x86_64-apple-darwin.tar.gz".to_string(),
                    browser_download_url: "https://example.com/cli-macos-x64.tar.gz".to_string(),
                },
                GithubAsset {
                    name: "galactica-cli-v0.1.0-aarch64-apple-darwin.tar.gz".to_string(),
                    browser_download_url: "https://example.com/cli-macos.tar.gz".to_string(),
                },
                GithubAsset {
                    name: "galactica-cli-v0.1.0-x86_64-pc-windows-msvc.zip".to_string(),
                    browser_download_url: "https://example.com/cli-windows.zip".to_string(),
                },
                GithubAsset {
                    name: "galactica-cli-v0.1.0-x86_64-unknown-linux-gnu.tar.gz".to_string(),
                    browser_download_url: "https://example.com/cli-linux.tar.gz".to_string(),
                },
            ],
        }
    }

    fn sample_detection(os: &str, cpu_arch: &str, accelerators: &[&str]) -> DetectionReport {
        DetectionReport {
            repo_root: ".".to_string(),
            model: "qwen3.5-4b".to_string(),
            hostname: "host".to_string(),
            os: os.to_string(),
            cpu_arch: cpu_arch.to_string(),
            execution_pool: "pool".to_string(),
            accelerators: accelerators.iter().map(|value| value.to_string()).collect(),
            runtime_backends: vec!["llama.cpp".to_string()],
            recommended_runtime: Some("llama.cpp".to_string()),
            recommended_quantization: Some("q4_k_m".to_string()),
            recommended_node_config: "config/dev/linux-node-agent.toml".to_string(),
            detected_local_ip: "127.0.0.1".to_string(),
            suggested_agent_endpoint: "http://127.0.0.1:50061".to_string(),
            notes: Vec::new(),
        }
    }

    #[test]
    fn selects_windows_cuda_llama_cpp_assets() {
        let assets = select_llama_cpp_assets(
            &sample_detection("windows", "x86_64", &["cuda"]),
            &sample_release(),
        )
        .unwrap();
        let names = assets
            .into_iter()
            .map(|asset| asset.name)
            .collect::<Vec<_>>();
        assert!(names.iter().any(|name| name.contains("win-cuda-12.4-x64")));
        assert!(names.iter().any(|name| name.contains("cudart-llama")));
    }

    #[test]
    fn selects_linux_cpu_asset_without_vulkan() {
        let assets = select_llama_cpp_assets(
            &sample_detection("linux", "x86_64", &["cpu"]),
            &sample_release(),
        )
        .unwrap();
        assert_eq!(assets.len(), 1);
        assert!(assets[0].name.contains("bin-ubuntu-x64"));
        assert!(!assets[0].name.contains("vulkan"));
    }

    #[test]
    fn selects_macos_x64_asset() {
        let assets = select_llama_cpp_assets(
            &sample_detection("macos", "x86_64", &["cpu"]),
            &sample_release(),
        )
        .unwrap();
        assert_eq!(assets.len(), 1);
        assert!(assets[0].name.contains("bin-macos-x64"));
    }

    #[test]
    fn selects_galactica_cli_release_asset_for_target() {
        let asset = select_galactica_cli_asset(&sample_release(), "x86_64-pc-windows-msvc", ".zip")
            .unwrap();
        assert!(asset.name.contains("x86_64-pc-windows-msvc"));
    }

    #[test]
    fn selects_galactica_cli_release_asset_for_intel_macos() {
        let asset = select_galactica_cli_asset(&sample_release(), "x86_64-apple-darwin", ".tar.gz")
            .unwrap();
        assert!(asset.name.contains("x86_64-apple-darwin"));
    }

    #[test]
    fn parses_workspace_package_values() {
        let cargo_toml = r#"
[workspace]
members = []

[workspace.package]
version = "0.1.0"
repository = "https://github.com/digiterialabs/galactica"
"#;
        assert_eq!(
            extract_workspace_string_value(cargo_toml, "version").as_deref(),
            Some("0.1.0")
        );
        assert_eq!(
            extract_workspace_string_value(cargo_toml, "repository").as_deref(),
            Some("https://github.com/digiterialabs/galactica")
        );
    }

    #[test]
    fn normalizes_repository_urls_and_release_tags() {
        assert_eq!(
            github_repo_slug_from_url("https://github.com/digiterialabs/galactica"),
            Some("digiterialabs/galactica".to_string())
        );
        assert_eq!(
            github_repo_slug_from_url("git@github.com:digiterialabs/galactica.git"),
            Some("digiterialabs/galactica".to_string())
        );
        assert_eq!(normalize_release_tag("v0.1.0"), "0.1.0");
        assert_eq!(normalize_release_tag("0.1.0"), "0.1.0");
    }

    #[test]
    fn maps_supported_release_targets() {
        assert_eq!(
            galactica_cli_target_triple("macos", "aarch64"),
            Some("aarch64-apple-darwin")
        );
        assert_eq!(
            galactica_cli_target_triple("macos", "x86_64"),
            Some("x86_64-apple-darwin")
        );
        assert_eq!(
            galactica_cli_target_triple("windows", "x86_64"),
            Some("x86_64-pc-windows-msvc")
        );
        assert_eq!(
            galactica_cli_target_triple("linux", "x86_64"),
            Some("x86_64-unknown-linux-gnu")
        );
    }
}
