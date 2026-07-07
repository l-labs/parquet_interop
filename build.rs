// build.rs — leave the L host symbols unresolved at link time.
// The library never links against L: it is dlopen'd INTO the L binary via
// `2:`, so ktn/sn/... must stay dangling until dyld/ld.so binds them
// against the already-loaded host process.  macOS ld64 rejects undefined
// symbols in a dylib by default, hence -undefined dynamic_lookup; ELF
// (Linux) shared objects allow undefined symbols with no flag at all.
fn main() {
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos") {
        println!(
            "cargo:rustc-cdylib-link-arg=-Wl,-undefined,dynamic_lookup"
        );
    }
}
