use std::hash::{Hash, Hasher};
use std::sync::Arc;

use polars::prelude::{
    DataType, DataTypeSelector, Selector, TimeUnit, TimeUnitSet, TimeZone, TimeZoneSet,
};
use polars_plan::dsl;

use crate::prelude::Wrap;
use crate::{RbResult, RbTypeError};

#[magnus::wrap(class = "Polars::RbSelector")]
#[repr(transparent)]
#[derive(Clone)]
pub struct RbSelector {
    pub inner: Selector,
}

impl From<Selector> for RbSelector {
    fn from(inner: Selector) -> Self {
        Self { inner }
    }
}

fn parse_time_unit_set(time_units: Vec<Wrap<TimeUnit>>) -> TimeUnitSet {
    let mut tu = TimeUnitSet::empty();
    for v in time_units {
        match v.0 {
            TimeUnit::Nanoseconds => tu |= TimeUnitSet::NANO_SECONDS,
            TimeUnit::Microseconds => tu |= TimeUnitSet::MICRO_SECONDS,
            TimeUnit::Milliseconds => tu |= TimeUnitSet::MILLI_SECONDS,
        }
    }
    tu
}

pub fn parse_datatype_selector(selector: &RbSelector) -> RbResult<DataTypeSelector> {
    selector.inner.clone().to_dtype_selector().ok_or_else(|| {
        RbTypeError::new_err(format!(
            "expected datatype based expression got '{}'",
            selector.inner
        ))
    })
}

impl RbSelector {
    pub fn union(&self, other: &Self) -> Self {
        Self {
            inner: self.inner.clone() | other.inner.clone(),
        }
    }

    pub fn difference(&self, other: &Self) -> Self {
        Self {
            inner: self.inner.clone() - other.inner.clone(),
        }
    }

    pub fn exclusive_or(&self, other: &Self) -> Self {
        Self {
            inner: self.inner.clone() ^ other.inner.clone(),
        }
    }

    pub fn intersect(&self, other: &Self) -> Self {
        Self {
            inner: self.inner.clone() & other.inner.clone(),
        }
    }

    pub fn by_dtype(dtypes: Vec<Wrap<DataType>>) -> Self {
        let dtypes = dtypes.into_iter().map(|x| x.0).collect::<Vec<_>>();
        dsl::dtype_cols(dtypes).as_selector().into()
    }

    pub fn by_name(names: Vec<String>, strict: bool) -> Self {
        dsl::by_name(names, strict).into()
    }

    pub fn by_index(indices: Vec<i64>, strict: bool) -> Self {
        Selector::ByIndex {
            indices: indices.into(),
            strict,
        }
        .into()
    }

    pub fn first(strict: bool) -> Self {
        Selector::ByIndex {
            indices: [0].into(),
            strict,
        }
        .into()
    }

    pub fn last(strict: bool) -> Self {
        Selector::ByIndex {
            indices: [-1].into(),
            strict,
        }
        .into()
    }

    pub fn matches(pattern: String) -> Self {
        Selector::Matches(pattern.into()).into()
    }

    pub fn enum_() -> Self {
        DataTypeSelector::Enum.as_selector().into()
    }

    pub fn categorical() -> Self {
        DataTypeSelector::Categorical.as_selector().into()
    }

    pub fn nested() -> Self {
        DataTypeSelector::Nested.as_selector().into()
    }

    pub fn list(inner_dst: Option<&Self>) -> RbResult<Self> {
        let inner_dst = match inner_dst {
            None => None,
            Some(inner_dst) => Some(Arc::new(parse_datatype_selector(inner_dst)?)),
        };
        Ok(DataTypeSelector::List(inner_dst).as_selector().into())
    }

    pub fn array(inner_dst: Option<&Self>, width: Option<usize>) -> RbResult<Self> {
        let inner_dst = match inner_dst {
            None => None,
            Some(inner_dst) => Some(Arc::new(parse_datatype_selector(inner_dst)?)),
        };
        Ok(DataTypeSelector::Array(inner_dst, width)
            .as_selector()
            .into())
    }

    pub fn struct_() -> Self {
        DataTypeSelector::Struct.as_selector().into()
    }

    pub fn integer() -> Self {
        DataTypeSelector::Integer.as_selector().into()
    }

    pub fn signed_integer() -> Self {
        DataTypeSelector::SignedInteger.as_selector().into()
    }

    pub fn unsigned_integer() -> Self {
        DataTypeSelector::UnsignedInteger.as_selector().into()
    }

    pub fn float() -> Self {
        DataTypeSelector::Float.as_selector().into()
    }

    pub fn decimal() -> Self {
        DataTypeSelector::Decimal.as_selector().into()
    }

    pub fn numeric() -> Self {
        DataTypeSelector::Numeric.as_selector().into()
    }

    pub fn temporal() -> Self {
        DataTypeSelector::Temporal.as_selector().into()
    }

    pub fn datetime(tu: Vec<Wrap<TimeUnit>>, tz: Vec<Wrap<Option<TimeZone>>>) -> Self {
        use TimeZoneSet as TZS;

        let mut allow_unset = false;
        let mut allow_set = false;
        let mut any_of: Vec<TimeZone> = Vec::new();

        let tu = parse_time_unit_set(tu);
        for t in tz {
            let t = t.0;
            match t {
                None => allow_unset = true,
                Some(s) if s.as_str() == "*" => allow_set = true,
                Some(t) => any_of.push(t),
            }
        }

        let tzs = match (allow_unset, allow_set) {
            (true, true) => TZS::Any,
            (false, true) => TZS::AnySet,
            (true, false) if any_of.is_empty() => TZS::Unset,
            (true, false) => TZS::UnsetOrAnyOf(any_of.into()),
            (false, false) => TZS::AnyOf(any_of.into()),
        };
        DataTypeSelector::Datetime(tu, tzs).as_selector().into()
    }

    pub fn duration(tu: Vec<Wrap<TimeUnit>>) -> Self {
        let tu = parse_time_unit_set(tu);
        DataTypeSelector::Duration(tu).as_selector().into()
    }

    pub fn object() -> Self {
        DataTypeSelector::Object.as_selector().into()
    }

    pub fn empty() -> Self {
        dsl::empty().into()
    }

    pub fn all() -> Self {
        dsl::all().into()
    }

    pub fn hash(&self) -> u64 {
        let mut hasher = std::hash::DefaultHasher::default();
        self.inner.hash(&mut hasher);
        hasher.finish()
    }
}
