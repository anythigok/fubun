use std::{
    env,
    path::{Path, PathBuf},
};

use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FubunPaths {
    pub runtime_directory: PathBuf,
    pub socket_path: PathBuf,
    pub data_directory: PathBuf,
    pub database_path: PathBuf,
}

#[derive(Debug, Error)]
pub enum PathError {
    #[error("XDG_RUNTIME_DIR is required")]
    MissingRuntimeDirectory,
    #[error("XDG_DATA_HOME is unset and HOME is unavailable")]
    MissingDataHome,
    #[error("{name} must be an absolute path: {path}")]
    NotAbsolute { name: &'static str, path: PathBuf },
}

impl FubunPaths {
    pub fn discover() -> Result<Self, PathError> {
        let runtime_home = env::var_os("XDG_RUNTIME_DIR")
            .map(PathBuf::from)
            .ok_or(PathError::MissingRuntimeDirectory)?;
        let data_home = env::var_os("XDG_DATA_HOME")
            .map(PathBuf::from)
            .or_else(|| env::var_os("HOME").map(|home| PathBuf::from(home).join(".local/share")))
            .ok_or(PathError::MissingDataHome)?;
        Self::from_xdg_roots(runtime_home, data_home)
    }

    pub fn from_xdg_roots(
        runtime_home: impl Into<PathBuf>,
        data_home: impl Into<PathBuf>,
    ) -> Result<Self, PathError> {
        let runtime_home = runtime_home.into();
        let data_home = data_home.into();
        ensure_absolute("XDG_RUNTIME_DIR", &runtime_home)?;
        ensure_absolute("XDG_DATA_HOME", &data_home)?;
        let runtime_directory = runtime_home.join("fubun");
        let data_directory = data_home.join("fubun");
        Ok(Self {
            socket_path: runtime_directory.join("core.sock"),
            database_path: data_directory.join("fubun.db"),
            runtime_directory,
            data_directory,
        })
    }
}

fn ensure_absolute(name: &'static str, path: &Path) -> Result<(), PathError> {
    if !path.is_absolute() {
        return Err(PathError::NotAbsolute {
            name,
            path: path.to_path_buf(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_canonical_xdg_paths() {
        let paths = FubunPaths::from_xdg_roots("/run/user/1000", "/home/test/.local/share")
            .expect("absolute paths");
        assert_eq!(
            paths.socket_path,
            PathBuf::from("/run/user/1000/fubun/core.sock")
        );
        assert_eq!(
            paths.database_path,
            PathBuf::from("/home/test/.local/share/fubun/fubun.db")
        );
    }
}
