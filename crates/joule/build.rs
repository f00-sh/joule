//! Embed Windows product icon + version resource into joule.exe.

fn main() {
    println!("cargo:rerun-if-changed=../../packaging/icons/joule.ico");
    println!("cargo:rerun-if-changed=build.rs");

    #[cfg(windows)]
    {
        embed_windows_icon();
    }
}

#[cfg(windows)]
fn embed_windows_icon() {
    let mut res = winres::WindowsResource::new();
    let ico =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../packaging/icons/joule.ico");
    if ico.is_file() {
        res.set_icon(ico.to_str().expect("ico path utf-8"));
    } else {
        println!(
            "cargo:warning=packaging/icons/joule.ico missing — shipping without embedded icon"
        );
    }
    res.set("ProductName", "joule");
    res.set("FileDescription", "joule — idle GPUs → open cluster");
    res.set("CompanyName", "f00-sh");
    res.set("LegalCopyright", "MIT License — f00-sh");
    res.set("OriginalFilename", "joule.exe");
    if let Err(e) = res.compile() {
        println!("cargo:warning=winres compile failed: {e}");
    }
}
