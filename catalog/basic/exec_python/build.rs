// Stamp an rpath to the probed interpreter's lib directory into any
// binary linking this package. In a container both stages share the
// distro python, whose lib dir is already on the loader path (the
// extra rpath is inert); on a host build the probed python is whatever
// `python3` resolves to (conda, pyenv, ...), whose libpython the
// loader cannot find without this.
pub fn main() {
    if let Some(lib_dir) = pyo3_build_config::get().lib_dir.as_ref() {
        println!("cargo:rustc-link-arg=-Wl,-rpath,{lib_dir}");
    }
}
