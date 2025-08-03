use std::sync::Arc;

use polars_dtype::categorical::Categories;

#[magnus::wrap(class = "Polars::RbCategories")]
#[repr(transparent)]
#[derive(Clone)]
pub struct RbCategories {
    categories: Arc<Categories>,
}

impl RbCategories {
    pub fn categories(&self) -> &Arc<Categories> {
        &self.categories
    }
}

impl RbCategories {
    pub fn global_categories() -> Self {
        Self {
            categories: Categories::global(),
        }
    }
}

impl From<Arc<Categories>> for RbCategories {
    fn from(categories: Arc<Categories>) -> Self {
        Self { categories }
    }
}
