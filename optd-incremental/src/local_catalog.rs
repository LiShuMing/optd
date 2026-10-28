// Copyright (c) 2023-2026 CMU Database Group
// Licensed under the MIT license. See LICENSE in the project root.

//! Limited single-host catalog used to qualify the CDC recovery protocol.
//! Immutable Iceberg metadata is published by an fsynced atomic local pointer.
//! Namespace mutation, deletion and remote locations are deliberately unsupported.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::spec::{TableMetadata, TableMetadataBuilder};
use iceberg::table::Table;
use iceberg::{
    Catalog, Error, ErrorKind, MetadataLocation, Namespace, NamespaceIdent, Result, TableCommit,
    TableCreation, TableIdent,
};

/// An fsynced local catalog. All processes must use this adapter's lock protocol.
#[derive(Debug)]
pub struct LocalCatalog {
    root: PathBuf,
}

fn io_error(e: impl std::fmt::Display) -> Error {
    Error::new(ErrorKind::Unexpected, e.to_string())
}
fn unsupported() -> Error {
    Error::new(
        ErrorKind::FeatureUnsupported,
        "operation outside the local CDC catalog contract",
    )
}
fn component(value: &str) -> Result<()> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(unsupported());
    }
    Ok(())
}

/// Hold this open for the entire writer lifetime; never unlink a live lock file.
#[derive(Debug)]
pub struct WriterLock {
    _file: File,
}
impl WriterLock {
    /// Acquire a nonblocking OS-held lock, released even after abrupt process death.
    pub fn acquire(path: &Path) -> std::result::Result<Self, crate::cdc::CdcError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        file.try_lock()
            .map_err(|_| crate::cdc::CdcError::Ownership)?;
        Ok(Self { _file: file })
    }
    /// Share an immutable scan with other readers; exclude physical maintenance.
    pub fn acquire_shared(path: &Path) -> std::result::Result<Self, crate::cdc::CdcError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        file.try_lock_shared()
            .map_err(|_| crate::cdc::CdcError::Ownership)?;
        Ok(Self { _file: file })
    }
}

impl LocalCatalog {
    /// Open/create a warehouse and canonicalize its identity across relative path aliases.
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        fs::create_dir_all(root.as_ref()).map_err(io_error)?;
        Ok(Self {
            root: fs::canonicalize(root.as_ref()).map_err(io_error)?,
        })
    }
    fn namespace_dir(&self, namespace: &NamespaceIdent) -> Result<PathBuf> {
        if namespace.len() != 1 {
            return Err(unsupported());
        }
        component(&namespace[0])?;
        Ok(self.root.join(&namespace[0]))
    }
    fn table_dir(&self, table: &TableIdent) -> Result<PathBuf> {
        component(table.name())?;
        Ok(self.namespace_dir(table.namespace())?.join(table.name()))
    }
    fn catalog_lock(&self) -> Result<WriterLock> {
        WriterLock::acquire(&self.root.join(".catalog.lock")).map_err(io_error)
    }
    /// Stable writer lock location for an already created output table UUID.
    pub fn writer_lock(
        &self,
        table_uuid: &str,
    ) -> std::result::Result<WriterLock, crate::cdc::CdcError> {
        component(table_uuid).map_err(|_| crate::cdc::CdcError::InvalidCheckpoint)?;
        WriterLock::acquire(&self.root.join(format!(".writer-{table_uuid}.lock")))
    }
    /// Acquire a shared read pin on the same lock namespace used by maintenance.
    pub fn reader_lock(&self, key: &str) -> std::result::Result<WriterLock, crate::cdc::CdcError> {
        component(key).map_err(|_| crate::cdc::CdcError::InvalidCheckpoint)?;
        WriterLock::acquire_shared(&self.root.join(format!(".writer-{key}.lock")))
    }
    pub(crate) fn consumer_dir(
        &self,
        identity: &str,
    ) -> std::result::Result<PathBuf, crate::cdc::CdcError> {
        component(identity).map_err(|_| crate::cdc::CdcError::InvalidCheckpoint)?;
        // Keep state outside table/data paths; Iceberg scans never discover these files.
        let dir = self.root.join("consumers").join(identity);
        fs::create_dir_all(&dir)?;
        File::open(
            dir.parent()
                .ok_or(crate::cdc::CdcError::InvalidCheckpoint)?,
        )?
        .sync_all()?;
        File::open(&self.root)?.sync_all()?;
        Ok(dir)
    }
    async fn read_table(&self, ident: &TableIdent) -> Result<Table> {
        let pointer = self.table_dir(ident)?.join("current");
        let location = fs::read_to_string(pointer).map_err(io_error)?;
        let metadata = TableMetadata::read_from(&FileIO::new_with_fs(), &location).await?;
        Table::builder()
            .identifier(ident.clone())
            .file_io(FileIO::new_with_fs())
            .metadata_location(location)
            .metadata(metadata)
            .runtime(iceberg::Runtime::try_current()?)
            .build()
    }
    fn publish(&self, ident: &TableIdent, metadata_location: &str) -> Result<()> {
        let dir = self.table_dir(ident)?;
        // Sync all newly created data/manifests before publishing their metadata pointer.
        fn sync_tree(path: &Path) -> std::io::Result<()> {
            for entry in fs::read_dir(path)? {
                let p = entry?.path();
                if p.is_dir() {
                    sync_tree(&p)?;
                } else if p.is_file() {
                    File::open(&p)?.sync_all()?;
                }
            }
            File::open(path)?.sync_all()
        }
        sync_tree(&dir).map_err(io_error)?;
        let staging = dir.join("current.pending");
        let mut file = File::create(&staging).map_err(io_error)?;
        file.write_all(metadata_location.as_bytes())
            .map_err(io_error)?;
        file.sync_all().map_err(io_error)?;
        fs::rename(staging, dir.join("current")).map_err(io_error)?;
        File::open(dir).and_then(|f| f.sync_all()).map_err(io_error)
    }
    /// Internal CAS for qualified output maintenance. Callers hold producer and scan locks.
    pub(crate) async fn publish_maintenance(
        &self,
        expected: &Table,
        metadata: TableMetadata,
    ) -> Result<Table> {
        let _lock = self.catalog_lock()?;
        let current = self.read_table(expected.identifier()).await?;
        if current.metadata_location() != expected.metadata_location()
            || metadata.uuid() != current.metadata().uuid()
            || metadata.current_schema() != current.metadata().current_schema()
            || metadata.properties() != current.metadata().properties()
            || metadata.current_snapshot().map(|s| s.sequence_number())
                < current
                    .metadata()
                    .current_snapshot()
                    .map(|s| s.sequence_number())
        {
            return Err(io_error("maintenance CAS conflict"));
        }
        let location = MetadataLocation::from_str(current.metadata_location_result()?)?
            .with_next_version()
            .try_with_new_metadata(&metadata)?;
        metadata.write_to(current.file_io(), &location).await?;
        self.publish(expected.identifier(), &location.to_string())?;
        self.read_table(expected.identifier()).await
    }
}

#[async_trait]
impl Catalog for LocalCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        if parent.is_some() {
            return Ok(vec![]);
        }
        let mut out = Vec::new();
        for entry in fs::read_dir(&self.root).map_err(io_error)? {
            let e = entry.map_err(io_error)?;
            if e.file_type().map_err(io_error)?.is_dir() {
                let name = e.file_name().into_string().map_err(|_| unsupported())?;
                component(&name)?;
                out.push(NamespaceIdent::new(name));
            }
        }
        out.sort();
        Ok(out)
    }
    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        if !properties.is_empty() {
            return Err(unsupported());
        }
        fs::create_dir_all(self.namespace_dir(namespace)?).map_err(io_error)?;
        File::open(&self.root)
            .and_then(|f| f.sync_all())
            .map_err(io_error)?;
        Ok(Namespace::new(namespace.clone()))
    }
    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        if !self.namespace_dir(namespace)?.is_dir() {
            return Err(io_error("namespace not found"));
        }
        Ok(Namespace::new(namespace.clone()))
    }
    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool> {
        Ok(self.namespace_dir(namespace)?.is_dir())
    }
    async fn update_namespace(&self, _: &NamespaceIdent, _: HashMap<String, String>) -> Result<()> {
        Err(unsupported())
    }
    async fn drop_namespace(&self, _: &NamespaceIdent) -> Result<()> {
        Err(unsupported())
    }
    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        let mut out = Vec::new();
        for entry in fs::read_dir(self.namespace_dir(namespace)?).map_err(io_error)? {
            let e = entry.map_err(io_error)?;
            if e.path().join("current").is_file() {
                let name = e.file_name().into_string().map_err(|_| unsupported())?;
                component(&name)?;
                out.push(TableIdent::new(namespace.clone(), name));
            }
        }
        out.sort_by(|a, b| a.name().cmp(b.name()));
        Ok(out)
    }
    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        mut creation: TableCreation,
    ) -> Result<Table> {
        let _lock = self.catalog_lock()?;
        let ident = TableIdent::new(namespace.clone(), creation.name.clone());
        let dir = self.table_dir(&ident)?;
        if dir.join("current").exists() {
            return Err(io_error("table already exists"));
        }
        let location = dir.to_str().ok_or_else(unsupported)?.to_string();
        if creation.location.as_ref().is_some_and(|l| l != &location) {
            return Err(unsupported());
        }
        creation.location = Some(location);
        fs::create_dir_all(dir.join("metadata")).map_err(io_error)?;
        let requested_schema = creation.schema.clone();
        let metadata = TableMetadataBuilder::from_table_creation(creation)?
            .build()?
            .metadata;
        // The upstream creation helper renumbers field ids. Restore the protocol's
        // declared ids in staging, removing the unused provisional schema before
        // the first metadata publication; no snapshot ever uses that provisional layout.
        let metadata = if metadata.current_schema().as_struct() != requested_schema.as_struct() {
            let provisional_id = metadata.current_schema_id();
            metadata
                .into_builder(None)
                .add_current_schema(requested_schema)?
                .remove_schemas(&[provisional_id])?
                .build()?
                .metadata
        } else {
            metadata
        };
        let metadata_location = MetadataLocation::try_new_with_metadata(&metadata)?;
        metadata
            .write_to(&FileIO::new_with_fs(), &metadata_location)
            .await?;
        self.publish(&ident, &metadata_location.to_string())?;
        self.read_table(&ident).await
    }
    async fn load_table(&self, ident: &TableIdent) -> Result<Table> {
        self.read_table(ident).await
    }
    async fn table_exists(&self, ident: &TableIdent) -> Result<bool> {
        Ok(self.table_dir(ident)?.join("current").exists())
    }
    async fn drop_table(&self, _: &TableIdent) -> Result<()> {
        Err(unsupported())
    }
    async fn purge_table(&self, _: &TableIdent) -> Result<()> {
        Err(unsupported())
    }
    async fn rename_table(&self, _: &TableIdent, _: &TableIdent) -> Result<()> {
        Err(unsupported())
    }
    async fn register_table(&self, _: &TableIdent, _: String) -> Result<Table> {
        Err(unsupported())
    }
    async fn update_table(&self, commit: TableCommit) -> Result<Table> {
        let _lock = self.catalog_lock()?;
        let ident = commit.identifier().clone();
        let current = self.read_table(&ident).await?;
        let staged = commit.apply(current.clone())?;
        // M1 deliberately retains all history. Enforce this in the qualified catalog,
        // rather than hoping an independently invoked expiration job follows the docs.
        if current
            .metadata()
            .snapshots()
            .any(|s| staged.metadata().snapshot_by_id(s.snapshot_id()).is_none())
        {
            return Err(unsupported());
        }
        let before = current
            .metadata()
            .current_snapshot()
            .map(|s| s.sequence_number());
        let after = staged
            .metadata()
            .current_snapshot()
            .map(|s| s.sequence_number());
        if after < before {
            return Err(unsupported());
        }
        let location = staged.metadata_location_result()?;
        staged
            .metadata()
            .write_to(staged.file_io(), &MetadataLocation::from_str(location)?)
            .await?;
        self.publish(&ident, location)?;
        // All mutations complete here; no deferred remote request can publish later.
        self.read_table(&ident).await
    }
}
