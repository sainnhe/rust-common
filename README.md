## Introduction

This is a template repository for rust projects. Edit `./Cargo.toml` to get started.

## Setup

Execute the following commands to install [git hooks](https://git-scm.com/docs/githooks):

1. `ln -s ../../githooks/pre-commit .git/hooks/`: Run checks and tests before committing.
2. `ln -s ../../githooks/commit-msg .git/hooks/`: Check whether the commit message conforms to [conventional commits](https://www.conventionalcommits.org/en/v1.0.0/).

The pre-commit hook requires the following tools to be installed:

1. [Clippy](https://github.com/rust-lang/rust-clippy): Some extra linting rules.
2. [cargo-deny](https://crates.io/crates/cargo-deny): Check for dependency security issues.
3. [cargo-udeps](https://crates.io/crates/cargo-udeps): Find unused dependencies.
4. [cargo-tarpaulin](https://crates.io/crates/cargo-tarpaulin): Test coverage examination.
