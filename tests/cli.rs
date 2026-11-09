use serde_json::Value;
use std::process::Command;

fn cli(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_gotomoch"))
        .args(args)
        .env("RUST_LOG", "warn")
        .output()
        .unwrap()
}

#[test]
fn validates_both_examples_without_authentication() {
    for file in ["examples/direct.toml", "examples/event-hub.toml"] {
        let output = cli(&["check", "--config", file]);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn preview_produces_typed_aggregates_and_correct_cost_totals() {
    let output = cli(&[
        "preview",
        "--config",
        "examples/direct.toml",
        "--input",
        "tests/fixtures/costs.csv",
    ]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records: Vec<Value> = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(records.len(), 3);
    assert_eq!(
        records
            .iter()
            .map(|r| r["Rows"].as_u64().unwrap())
            .sum::<u64>(),
        5
    );
    assert!(
        records
            .iter()
            .all(|r| r["Cost"].is_number() && r["Dimensions"].is_object())
    );
    let compute = records
        .iter()
        .find(|r| {
            r["Currency"] == "USD" && r["Dimensions"]["ConsumedService"] == "Microsoft.Compute"
        })
        .unwrap();
    assert_eq!(compute["Cost"].to_string(), "0.25");
    assert_eq!(compute["Rows"], 3);
    assert!(compute["RecordId"].as_str().unwrap().len() == 64);
}

#[test]
fn invalid_inputs_fail_without_success_output() {
    for args in [
        vec!["check", "--config", "README.md"],
        vec![
            "preview",
            "--config",
            "examples/direct.toml",
            "--input",
            "README.md",
        ],
    ] {
        let output = cli(&args);
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert!(String::from_utf8_lossy(&output.stderr).contains("error:"));
    }
}
