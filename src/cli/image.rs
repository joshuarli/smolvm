//! Internal image-material operations shared by local orchestration tools.

use clap::{Args, Subcommand, ValueEnum};

/// Image operations that do not create or inspect a machine.
#[derive(Subcommand, Debug)]
pub enum ImageCmd {
    /// Resolve an immutable registry image into a verified local archive.
    #[command(hide = true)]
    Materialize(MaterializeCmd),
}

impl ImageCmd {
    /// Run the selected image operation.
    pub fn run(self) -> smolvm::Result<()> {
        match self {
            Self::Materialize(command) => command.run(),
        }
    }
}

/// Resolve a digest-pinned registry image into smolvm's local archive cache.
#[derive(Args, Debug)]
pub struct MaterializeCmd {
    /// Immutable OCI image reference, including its sha256 digest.
    #[arg(long)]
    pub reference: String,

    /// Stable machine-readable result record.
    #[arg(long, value_enum, default_value = "tsv")]
    pub format: ImageMaterialFormat,
}

/// The sole representation of an image-materialization result.
#[derive(Clone, Copy, Debug, ValueEnum, PartialEq, Eq)]
pub enum ImageMaterialFormat {
    /// Stable tab-separated machine-readable record.
    Tsv,
}

impl MaterializeCmd {
    fn run(self) -> smolvm::Result<()> {
        debug_assert!(matches!(self.format, ImageMaterialFormat::Tsv));
        let material =
            smolvm::data::oci_materializer::materialize_registry_archive(&self.reference)?;
        let archive = material.archive_path.to_str().ok_or_else(|| {
            smolvm::Error::config(
                "image materialize",
                "prepared archive path is not valid UTF-8",
            )
        })?;
        if [
            material.source_reference.as_str(),
            material.source_digest.as_str(),
            archive,
            material.archive_blake3_digest.as_str(),
        ]
        .iter()
        .any(|value| value.contains(['\t', '\r', '\n']))
        {
            return Err(smolvm::Error::config(
                "image materialize",
                "material result contains a TSV delimiter",
            ));
        }
        println!(
            "image-material-v1\t{}\t{}\t{}\t{}",
            material.source_reference,
            material.source_digest,
            archive,
            material.archive_blake3_digest,
        );
        Ok(())
    }
}

