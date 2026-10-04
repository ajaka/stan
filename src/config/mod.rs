// The `config::config` path is redundant to clippy's taste, but renaming it
// would churn the import path in every module that reads settings.
#![allow(clippy::module_inception)]

pub mod config;
