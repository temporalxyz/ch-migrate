use proc_macro::TokenStream;
use std::path::PathBuf;

use quote::quote;

/// Discovers migration `.sql` files at compile time, embeds them via `include_str!`,
/// computes SHA-256 checksums, and returns a static `Migrator`.
///
/// # Usage
///
/// ```ignore
/// // Default: reads from ./migrations relative to Cargo.toml
/// static MIGRATOR: ch_migrate::Migrator = ch_migrate::migrate!();
///
/// // Custom path:
/// static MIGRATOR: ch_migrate::Migrator = ch_migrate::migrate!("db/migrations");
/// ```
#[proc_macro]
pub fn migrate(input: TokenStream) -> TokenStream {
    match expand(input) {
        Ok(ts) => ts.into(),
        Err(e) => {
            let msg = e.to_string();
            quote! { compile_error!(#msg) }.into()
        }
    }
}

fn expand(input: TokenStream) -> Result<proc_macro2::TokenStream, Box<dyn std::error::Error>> {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")?;

    let dir = if input.is_empty() {
        PathBuf::from(&manifest_dir).join("migrations")
    } else {
        let lit: syn::LitStr = syn::parse(input)?;
        PathBuf::from(&manifest_dir).join(lit.value())
    };

    if !dir.is_dir() {
        return Err(format!("migrations directory not found: {}", dir.display()).into());
    }

    // Discover migration files using the core crate's blocking resolver.
    // This also computes checksums via ch_migrate_core::checksum::compute.
    let migrations = ch_migrate_core::source::resolve_blocking(&dir)?;

    let migration_tokens: Vec<proc_macro2::TokenStream> = migrations
        .iter()
        .map(|(migration, path)| {
            let version = migration.version;
            let description = &*migration.description;

            // Canonicalize path for include_str!
            let canonical = path
                .canonicalize()
                .map_err(|e| format!("failed to canonicalize {}: {}", path.display(), e))?;
            let path_str = canonical
                .to_str()
                .ok_or_else(|| format!("non-UTF-8 path: {}", canonical.display()))?;

            // Use the checksum already computed by resolve_blocking.
            let checksum_lit: Vec<proc_macro2::TokenStream> =
                migration.checksum.iter().map(|b| quote! { #b }).collect();

            // Paths go through ::ch_migrate:: (the facade crate) so that
            // users only need `ch-migrate` as a direct dependency, not
            // `ch-migrate-core`.
            Ok(quote! {
                ::ch_migrate::Migration {
                    version: #version,
                    description: ::std::borrow::Cow::Borrowed(#description),
                    sql: ::std::borrow::Cow::Borrowed(include_str!(#path_str)),
                    checksum: ::std::borrow::Cow::Borrowed(&[ #(#checksum_lit),* ]),
                }
            })
        })
        .collect::<Result<Vec<_>, String>>()?;

    let count = migration_tokens.len();

    Ok(quote! {
        {
            const MIGRATIONS: [::ch_migrate::Migration; #count] = [
                #(#migration_tokens),*
            ];
            ::ch_migrate::Migrator::new(&MIGRATIONS)
        }
    })
}
