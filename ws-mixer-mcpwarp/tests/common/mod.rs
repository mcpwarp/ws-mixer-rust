//! Locates the mcpwarp fixture corpus of the spec checkout.

use std::path::{Path, PathBuf};

/// `<spec>/fixtures/mcpwarp`, from `WSMIXER_SPEC_DIR` (a ws-mixer-spec
/// checkout, or its `spec/` directory) or the repo's `.spec/` checkout
/// (`make fetch-spec`). A relative `WSMIXER_SPEC_DIR` is taken from the repo
/// root, as for ws-mixer's own fixture tests run from there. `None` when
/// neither exists: a printed note locally, a panic when `CI` is set, so CI
/// never passes on a missing corpus.
pub fn mcpwarp_fixtures() -> Option<PathBuf> {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    if let Some(dir) = std::env::var_os("WSMIXER_SPEC_DIR") {
        let dir = repo.join(dir);
        return Some(resolve(&dir).unwrap_or_else(|| {
            panic!(
                "WSMIXER_SPEC_DIR={}: found neither spec/fixtures/mcpwarp nor fixtures/mcpwarp under it",
                dir.display()
            )
        }));
    }
    if let Some(dir) = resolve(&repo.join(".spec")) {
        return Some(dir);
    }
    let note = "no mcpwarp fixture corpus (set WSMIXER_SPEC_DIR or run `make fetch-spec`)";
    if std::env::var_os("CI").is_some() {
        panic!("{note}");
    }
    println!("skipping: {note}");
    None
}

fn resolve(dir: &Path) -> Option<PathBuf> {
    [
        dir.join("spec/fixtures/mcpwarp"),
        dir.join("fixtures/mcpwarp"),
    ]
    .into_iter()
    .find(|d| d.is_dir())
}

/// The `*.json` files in `dir`, sorted.
pub fn json_files(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<_> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "json"))
        .collect();
    files.sort();
    files
}

/// Parses a fixture file, keeping key order.
pub fn read_json(path: &Path) -> serde_json::Value {
    let text = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}
