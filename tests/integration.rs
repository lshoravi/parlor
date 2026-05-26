use scheme_rs::runtime::Runtime;
use scheme_rs_cml as _;
use std::path::PathBuf;

fn run_scheme_test(filename: &str) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let scheme_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("scheme");
        unsafe { std::env::set_var("SCHEME_RS_LOAD_PATH", &scheme_dir) };

        let runtime = Runtime::new();
        let test_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join(filename);
        runtime
            .run_program(&test_path)
            .await
            .expect(&format!("scheme test {filename} failed"));
    });
}

#[test]
fn test_cml_basic() {
    run_scheme_test("cml_basic.scm");
}

#[test]
fn test_cml_channels() {
    run_scheme_test("cml_channels.scm");
}

#[test]
fn test_cml_conditions() {
    run_scheme_test("cml_conditions.scm");
}

#[test]
fn test_cml_compose() {
    run_scheme_test("cml_compose.scm");
}
