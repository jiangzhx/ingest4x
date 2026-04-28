use clap::{ArgAction, Args, Parser, Subcommand};
use ingest4x::logging::init_logging;
use ingest4x::server;
use ingest4x::settings::Settings;
use std::sync::Arc;

#[derive(Parser, Debug)]
#[command(name = "ingest4x")]
#[command(about = "日志收集服务器", long_about = None)]
struct Cli {
    #[arg(short, long, required = false)]
    config: Option<String>,

    #[arg(short = 'v', long = "version", action = ArgAction::SetTrue)]
    version: bool,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    Server(ServerCli),
}

#[derive(Args, Debug, Default)]
struct ServerCli {
    #[arg(short, long, required = false)]
    config: Option<String>,

    #[arg(short = 'v', long = "version", action = ArgAction::SetTrue)]
    version: bool,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let cli = Cli::parse();

    if cli.version {
        println!("{}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }

    match cli.command {
        Some(Command::Server(server_cli)) => {
            run_server(server_cli.config.or(cli.config), server_cli.version).await
        }
        None => run_server(cli.config, false).await,
    }
}

async fn run_server(config: Option<String>, version: bool) -> std::io::Result<()> {
    if version {
        println!("{}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }

    let settings = Settings::new(config).expect("无法加载配置文件");
    init_logging(&settings).map_err(|err| std::io::Error::other(err.to_string()))?;

    server::start(Arc::new(settings)).await
}
