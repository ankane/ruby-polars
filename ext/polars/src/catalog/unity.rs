use magnus::value::{Lazy, ReprValue};
use magnus::{IntoValue, Module, RArray, RClass, RHash, RModule, Ruby, Value};
use polars::prelude::{PlHashMap, PlSmallStr};
use polars_io::catalog::unity::client::{CatalogClient, CatalogClientBuilder};
use polars_io::catalog::unity::models::CatalogInfo;
use polars_io::pl_async;

use crate::rb_modules::polars;
use crate::utils::to_rb_err;
use crate::RbResult;

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

    pub fn list_catalogs(&self) -> RbResult<Value> {
        let v = pl_async::get_runtime()
            .block_in_place_on(self.client().list_catalogs())
            .map_err(to_rb_err)?;

        let mut opt_err = None;

        let out = RArray::from_iter(v.into_iter().map(|x| {
            let v = catalog_info_to_rbobject(x);
            if let Ok(v) = v {
                Some(v)
            } else {
                opt_err.replace(v);
                None
            }
        }));

        opt_err.transpose()?;

        Ok(out.into_value())
    }
}

impl RbCatalogClient {
    fn client(&self) -> &CatalogClient {
        &self.0
    }
}

fn catalog_info_to_rbobject(
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
    let dict = RHash::new();

    let properties = properties_to_rbobject(properties);
    let options = properties_to_rbobject(options);

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

    Ruby::get()
        .unwrap()
        .get_inner(&CATALOG_INFO_CLS)
        .funcall("new", (dict,))
}

fn properties_to_rbobject(properties: PlHashMap<PlSmallStr, String>) -> RHash {
    let dict = RHash::new();

    for (key, value) in properties.into_iter() {
        dict.aset(key.as_str(), value).unwrap();
    }

    dict
}
