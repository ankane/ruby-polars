use crate::RbExpr;

impl RbExpr {
    pub fn struct_field_by_index(&self, index: i64) -> Self {
        self.inner.clone().struct_().field_by_index(index).into()
    }

    pub fn struct_field_by_name(&self, name: String) -> Self {
        self.inner.clone().struct_().field_by_name(&name).into()
    }

    pub fn struct_rename_fields(&self, names: Vec<String>) -> Self {
        self.inner.clone().struct_().rename_fields(names).into()
    }

    pub fn struct_json_encode(&self) -> Self {
        self.inner.clone().struct_().json_encode().into()
    }
}
