use anyhow::Context;
use clap::Args;
use reservoir::DamIterator;
use std::path::PathBuf;
use tracing::info;

#[derive(Args)]
pub struct DamCommand {
    /// The path to the dam log
    dam_path: PathBuf,

    /// Tail the log file until canceled
    #[arg(short, long, help = "Follow/tail the log file")]
    follow: bool,

    #[arg(short, long, help = "Prettify the output")]
    pretty: bool,
}

impl DamCommand {
    pub fn execute(self) -> anyhow::Result<()> {
        let dam_iter = DamIterator::new(&self.dam_path).context("Could not open dam iter")?;
        print_from_iter(self.pretty, self.follow, dam_iter);
        Ok(())
    }
}

fn print_from_iter(pretty: bool, tail: bool, mut iter: DamIterator) {
    loop {
        while let Some((txn, offset)) = iter.next() {
            if pretty {
                info!("offset={offset} {txn:#?}");
            } else {
                info!("offset={offset} {txn:?}");
            }
        }

        if !tail {
            info!("Reached end of the dam log. Exiting.");
            break;
        }
    }
}
