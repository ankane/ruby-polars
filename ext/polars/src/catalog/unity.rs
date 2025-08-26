use std::str::FromStr;

use magnus::value::{Lazy, ReprValue};
use magnus::{IntoValue, Module, RClass, RHash, RModule, Ruby, Value};
use polars::prelude::{PlHashMap, PlSmallStr, Schema};
use polars_io::catalog::unity::client::{CatalogClient, CatalogClientBuilder};
use polars_io::catalog::unity::models::{
    CatalogInfo, ColumnInfo, DataSourceFormat, NamespaceInfo, TableInfo, TableType,
};
use polars_io::catalog::unity::schema::parse_type_json_str;
use polars_io::pl_async;

use crate::rb_modules::polars;
use crate::utils::to_rb_err;
use crate::{RbResult, RbValueError, Wrap};

macro_rules! rbdict_insert_keys {
    ($dict:expr, {$a:expr}) => {
        $dict.aset(stringify!($a), $a)?;
    };

    ($dict:expr, {$a:expr, $($args:expr),+}) => {
        rbdict_insert_keys!($dict, { $a });
        rbdict_insert_keys!($dict, { $($args),+ });
    };

    ($dict:expr, {$a:expr, $($args:expr),+,}) => {
        rbdict_insert_keys!($dict, {$a, $($args),+});
    };
}

static CATALOG_INFO_CLS: Lazy<RClass> = Lazy::new(|_| {
    polars()
        .const_get::<_, RClass>("Catalog")
        .unwrap()
        .const_get::<_, RModule>("Unity")
        .unwrap()
        .const_get("CatalogInfo")
        .unwrap()
});

static NAMESPACE_INFO_CLS: Lazy<RClass> = Lazy::new(|_| {
    polars()
        .const_get::<_, RClass>("Catalog")
        .unwrap()
        .const_get::<_, RModule>("Unity")
        .unwrap()
        .const_get("NamespaceInfo")
        .unwrap()
});

static TABLE_INFO_CLS: Lazy<RClass> = Lazy::new(|_| {
    polars()
        .const_get::<_, RClass>("Catalog")
        .unwrap()
        .const_get::<_, RModule>("Unity")
        .unwrap()
        .const_get("TableInfo")
        .unwrap()
});

static COLUMN_INFO_CLS: Lazy<RClass> = Lazy::new(|_| {
    polars()
        .const_get::<_, RClass>("Catalog")
        .unwrap()
        .const_get::<_, RModule>("Unity")
        .unwrap()
        .const_get("ColumnInfo")
        .unwrap()
});

#[magnus::wrap(class = "Polars::RbCatalogClient")]
pub struct RbCatalogClient(CatalogClient);

impl RbCatalogClient {
    pub fn new(workspace_url: String, bearer_token: Option<String>) -> RbResult<Self> {
        let builder = CatalogClientBuilder::new().with_workspace_url(workspace_url);

        let builder = if let Some(bearer_token) = bearer_token {
            builder.with_bearer_token(bearer_token)
        } else {
            builder
        };

        builder.build().map(RbCatalogClient).map_err(to_rb_err)
    }

    pub fn list_catalogs(ruby: &Ruby, rb_self: &Self) -> RbResult<Value> {
        let v = pl_async::get_runtime()
            .block_in_place_on(rb_self.client().list_catalogs())
            .map_err(to_rb_err)?;

        let mut opt_err = None;

        let out = ruby.ary_from_iter(v.into_iter().map(|x| {
            let v = catalog_info_to_rbobject(ruby, x);
            if let Ok(v) = v {
                Some(v)
            } else {
                opt_err.replace(v);
                None
            }
        }));

        opt_err.transpose()?;

        Ok(out.into_value_with(ruby))
    }

    pub fn list_namespaces(ruby: &Ruby, rb_self: &Self, catalog_name: String) -> RbResult<Value> {
        let v = pl_async::get_runtime()
            .block_in_place_on(rb_self.client().list_namespaces(&catalog_name))
            .map_err(to_rb_err)?;

        let mut opt_err = None;

        let out = ruby.ary_from_iter(v.into_iter().map(|x| {
            let v = namespace_info_to_rbobject(ruby, x);
            match v {
                Ok(v) => Some(v),
                Err(_) => {
                    opt_err.replace(v);
                    None
                }
            }
        }));

        opt_err.transpose()?;

        Ok(out.into_value_with(ruby))
    }

    pub fn list_tables(
        ruby: &Ruby,
        rb_self: &Self,
        catalog_name: String,
        namespace: String,
    ) -> RbResult<Value> {
        let v = pl_async::get_runtime()
            .block_in_place_on(rb_self.client().list_tables(&catalog_name, &namespace))
            .map_err(to_rb_err)?;

        let mut opt_err = None;

        let out = ruby
            .ary_from_iter(v.into_iter().map(|table_info| {
                let v = table_info_to_rbobject(ruby, table_info);

                if let Ok(v) = v {
                    Some(v)
                } else {
                    opt_err.replace(v);
                    None
                }
            }))
            .into_value_with(ruby);

        opt_err.transpose()?;

        Ok(out)
    }

    pub fn get_table_info(
        ruby: &Ruby,
        rb_self: &Self,
        table_name: String,
        catalog_name: String,
        namespace: String,
    ) -> RbResult<Value> {
        let table_info = pl_async::get_runtime()
            .block_in_place_on(rb_self.client().get_table_info(
                &table_name,
                &catalog_name,
                &namespace,
            ))
            .map_err(to_rb_err)?;

        table_info_to_rbobject(ruby, table_info)
    }

    pub fn create_catalog(
        ruby: &Ruby,
        rb_self: &Self,
        catalog_name: String,
        comment: Option<String>,
        storage_root: Option<String>,
    ) -> RbResult<Value> {
        let catalog_info = pl_async::get_runtime()
            .block_in_place_on(rb_self.client().create_catalog(
                &catalog_name,
                comment.as_deref(),
                storage_root.as_deref(),
            ))
            .map_err(to_rb_err)?;

        catalog_info_to_rbobject(ruby, catalog_info)
    }

    pub fn delete_catalog(&self, catalog_name: String, force: bool) -> RbResult<()> {
        pl_async::get_runtime()
            .block_in_place_on(self.client().delete_catalog(&catalog_name, force))
            .map_err(to_rb_err)
    }

    pub fn create_namespace(
        ruby: &Ruby,
        rb_self: &Self,
        catalog_name: String,
        namespace: String,
        comment: Option<String>,
        storage_root: Option<String>,
    ) -> RbResult<Value> {
        let namespace_info = pl_async::get_runtime()
            .block_in_place_on(rb_self.client().create_namespace(
                &catalog_name,
                &namespace,
                comment.as_deref(),
                storage_root.as_deref(),
            ))
            .map_err(to_rb_err)?;

        namespace_info_to_rbobject(ruby, namespace_info)
    }

    pub fn delete_namespace(
        &self,
        catalog_name: String,
        namespace: String,
        force: bool,
    ) -> RbResult<()> {
        pl_async::get_runtime()
            .block_in_place_on(
                self.client()
                    .delete_namespace(&catalog_name, &namespace, force),
            )
            .map_err(to_rb_err)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_table(
        ruby: &Ruby,
        rb_self: &Self,
        catalog_name: String,
        namespace: String,
        table_name: String,
        schema: Option<Wrap<Schema>>,
        table_type: String,
        data_source_format: Option<String>,
        comment: Option<String>,
        storage_root: Option<String>,
        properties: Vec<(String, String)>,
    ) -> RbResult<Value> {
        let table_info = pl_async::get_runtime()
            .block_in_place_on(
                rb_self.client().create_table(
                    &catalog_name,
                    &namespace,
                    &table_name,
                    schema.as_ref().map(|x| &x.0),
                    &TableType::from_str(&table_type)
                        .map_err(|e| RbValueError::new_err(e.to_string()))?,
                    data_source_format
                        .as_deref()
                        .map(DataSourceFormat::from_str)
                        .transpose()
                        .map_err(|e| RbValueError::new_err(e.to_string()))?
                        .as_ref(),
                    comment.as_deref(),
                    storage_root.as_deref(),
                    &mut properties.iter().map(|(a, b)| (a.as_str(), b.as_str())),
                ),
            )
            .map_err(to_rb_err)?;

        table_info_to_rbobject(ruby, table_info)
    }

    pub fn delete_table(
        &self,
        catalog_name: String,
        namespace: String,
        table_name: String,
    ) -> RbResult<()> {
        pl_async::get_runtime()
            .block_in_place_on(
                self.client()
                    .delete_table(&catalog_name, &namespace, &table_name),
            )
            .map_err(to_rb_err)
    }

    pub fn type_json_to_polars_type(ruby: &Ruby, type_json: String) -> RbResult<Value> {
        Ok(Wrap(parse_type_json_str(&type_json).map_err(to_rb_err)?).into_value_with(ruby))
    }
}

impl RbCatalogClient {
    fn client(&self) -> &CatalogClient {
        &self.0
    }
}

fn catalog_info_to_rbobject(
    ruby: &Ruby,
    CatalogInfo {
        name,
        comment,
        storage_location,
        properties,
        options,
        created_at,
        created_by,
        updated_at,
        updated_by,
    }: CatalogInfo,
) -> RbResult<Value> {
    let dict = ruby.hash_new();

    let properties = properties_to_rbobject(ruby, properties);
    let options = properties_to_rbobject(ruby, options);

    rbdict_insert_keys!(dict, {
        name,
        comment,
        storage_location,
        properties,
        options,
        created_at,
        created_by,
        updated_at,
        updated_by
    });

    ruby.get_inner(&CATALOG_INFO_CLS).funcall("new", (dict,))
}

fn namespace_info_to_rbobject(
    ruby: &Ruby,
    NamespaceInfo {
        name,
        comment,
        properties,
        storage_location,
        created_at,
        created_by,
        updated_at,
        updated_by,
    }: NamespaceInfo,
) -> RbResult<Value> {
    let dict = ruby.hash_new();

    let properties = properties_to_rbobject(ruby, properties);

    rbdict_insert_keys!(dict, {
        name,
        comment,
        properties,
        storage_location,
        created_at,
        created_by,
        updated_at,
        updated_by
    });

    ruby.get_inner(&NAMESPACE_INFO_CLS).funcall("new", (dict,))
}

fn table_info_to_rbobject(ruby: &Ruby, table_info: TableInfo) -> RbResult<Value> {
    let TableInfo {
        name,
        table_id,
        table_type,
        comment,
        storage_location,
        data_source_format,
        columns,
        properties,
        created_at,
        created_by,
        updated_at,
        updated_by,
    } = table_info;

    let column_info_cls = ruby.get_inner(&COLUMN_INFO_CLS);

    let columns = columns
        .map(|columns| {
            ruby.ary_try_from_iter(columns.into_iter().map(
                |ColumnInfo {
                     name,
                     type_name,
                     type_text,
                     type_json,
                     position,
                     comment,
                     partition_index,
                 }| {
                    let dict = ruby.hash_new();

                    let name = name.as_str();
                    let type_name = type_name.as_str();
                    let type_text = type_text.as_str();

                    rbdict_insert_keys!(dict, {
                        name,
                        type_name,
                        type_text,
                        type_json,
                        position,
                        comment,
                        partition_index,
                    });

                    column_info_cls.funcall::<_, _, Value>("new", (dict,))
                },
            ))
        })
        .transpose()?;

    let dict = ruby.hash_new();

    let data_source_format = data_source_format.map(|x| x.to_string());
    let table_type = table_type.to_string();
    let properties = properties_to_rbobject(ruby, properties);

    rbdict_insert_keys!(dict, {
        name,
        comment,
        table_id,
        table_type,
        storage_location,
        data_source_format,
        columns,
        properties,
        created_at,
        created_by,
        updated_at,
        updated_by,
    });

    ruby.get_inner(&TABLE_INFO_CLS).funcall("new", (dict,))
}

fn properties_to_rbobject(ruby: &Ruby, properties: PlHashMap<PlSmallStr, String>) -> RHash {
    let dict = ruby.hash_new();

    for (key, value) in properties.into_iter() {
        dict.aset(key.as_str(), value).unwrap();
    }

    dict
}
