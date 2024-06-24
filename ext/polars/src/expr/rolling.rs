use polars::prelude::*;
use std::any::Any;

use crate::conversion::Wrap;
use crate::RbExpr;

impl RbExpr {
    pub fn rolling_sum(
        &self,
        window_size: usize,
        weights: Option<Vec<f64>>,
        min_periods: Option<usize>,
        center: bool,
    ) -> Self {
        let min_periods = min_periods.unwrap_or(window_size);
        let options = RollingOptionsFixedWindow {
            window_size,
            weights,
            min_periods,
            center,
            ..Default::default()
        };
        self.inner.clone().rolling_sum(options).into()
    }

    pub fn rolling_sum_by(
        &self,
        by: &RbExpr,
        window_size: String,
        min_periods: usize,
        closed: Wrap<ClosedWindow>,
    ) -> Self {
        let options = RollingOptionsDynamicWindow {
            window_size: Duration::parse(&window_size),
            min_periods,
            closed_window: closed.0,
            fn_params: None,
        };
        self.inner
            .clone()
            .rolling_sum_by(by.inner.clone(), options)
            .into()
    }

    pub fn rolling_min(
        &self,
        window_size: usize,
        weights: Option<Vec<f64>>,
        min_periods: Option<usize>,
        center: bool,
    ) -> Self {
        let min_periods = min_periods.unwrap_or(window_size);
        let options = RollingOptionsFixedWindow {
            window_size,
            weights,
            min_periods,
            center,
            ..Default::default()
        };
        self.inner.clone().rolling_min(options).into()
    }

    pub fn rolling_min_by(
        &self,
        by: &RbExpr,
        window_size: String,
        min_periods: usize,
        closed: Wrap<ClosedWindow>,
    ) -> Self {
        let options = RollingOptionsDynamicWindow {
            window_size: Duration::parse(&window_size),
            min_periods,
            closed_window: closed.0,
            fn_params: None,
        };
        self.inner
            .clone()
            .rolling_min_by(by.inner.clone(), options)
            .into()
    }

    pub fn rolling_max(
        &self,
        window_size: usize,
        weights: Option<Vec<f64>>,
        min_periods: Option<usize>,
        center: bool,
    ) -> Self {
        let min_periods = min_periods.unwrap_or(window_size);
        let options = RollingOptionsFixedWindow {
            window_size,
            weights,
            min_periods,
            center,
            ..Default::default()
        };
        self.inner.clone().rolling_max(options).into()
    }

    pub fn rolling_max_by(
        &self,
        by: &RbExpr,
        window_size: String,
        min_periods: usize,
        closed: Wrap<ClosedWindow>,
    ) -> Self {
        let options = RollingOptionsDynamicWindow {
            window_size: Duration::parse(&window_size),
            min_periods,
            closed_window: closed.0,
            fn_params: None,
        };
        self.inner
            .clone()
            .rolling_max_by(by.inner.clone(), options)
            .into()
    }

    pub fn rolling_mean(
        &self,
        window_size: usize,
        weights: Option<Vec<f64>>,
        min_periods: Option<usize>,
        center: bool,
    ) -> Self {
        let min_periods = min_periods.unwrap_or(window_size);
        let options = RollingOptionsFixedWindow {
            window_size,
            weights,
            min_periods,
            center,
            ..Default::default()
        };

        self.inner.clone().rolling_mean(options).into()
    }

    pub fn rolling_mean_by(
        &self,
        by: &RbExpr,
        window_size: String,
        min_periods: usize,
        closed: Wrap<ClosedWindow>,
    ) -> Self {
        let options = RollingOptionsDynamicWindow {
            window_size: Duration::parse(&window_size),
            min_periods,
            closed_window: closed.0,
            fn_params: None,
        };

        self.inner
            .clone()
            .rolling_mean_by(by.inner.clone(), options)
            .into()
    }

    pub fn rolling_std(
        &self,
        window_size: usize,
        weights: Option<Vec<f64>>,
        min_periods: Option<usize>,
        center: bool,
        ddof: u8,
    ) -> Self {
        let min_periods = min_periods.unwrap_or(window_size);
        let options = RollingOptionsFixedWindow {
            window_size,
            weights,
            min_periods,
            center,
            fn_params: Some(Arc::new(RollingVarParams { ddof }) as Arc<dyn Any + Send + Sync>),
        };

        self.inner.clone().rolling_std(options).into()
    }

    pub fn rolling_std_by(
        &self,
        by: &RbExpr,
        window_size: String,
        min_periods: usize,
        closed: Wrap<ClosedWindow>,
        ddof: u8,
    ) -> Self {
        let options = RollingOptionsDynamicWindow {
            window_size: Duration::parse(&window_size),
            min_periods,
            closed_window: closed.0,
            fn_params: Some(Arc::new(RollingVarParams { ddof }) as Arc<dyn Any + Send + Sync>),
        };

        self.inner
            .clone()
            .rolling_std_by(by.inner.clone(), options)
            .into()
    }

    pub fn rolling_var(
        &self,
        window_size: usize,
        weights: Option<Vec<f64>>,
        min_periods: Option<usize>,
        center: bool,
        ddof: u8,
    ) -> Self {
        let min_periods = min_periods.unwrap_or(window_size);
        let options = RollingOptionsFixedWindow {
            window_size,
            weights,
            min_periods,
            center,
            fn_params: Some(Arc::new(RollingVarParams { ddof }) as Arc<dyn Any + Send + Sync>),
        };

        self.inner.clone().rolling_var(options).into()
    }

    pub fn rolling_var_by(
        &self,
        by: &RbExpr,
        window_size: String,
        min_periods: usize,
        closed: Wrap<ClosedWindow>,
        ddof: u8,
    ) -> Self {
        let options = RollingOptionsDynamicWindow {
            window_size: Duration::parse(&window_size),
            min_periods,
            closed_window: closed.0,
            fn_params: Some(Arc::new(RollingVarParams { ddof }) as Arc<dyn Any + Send + Sync>),
        };

        self.inner
            .clone()
            .rolling_var_by(by.inner.clone(), options)
            .into()
    }

    pub fn rolling_median(
        &self,
        window_size: usize,
        weights: Option<Vec<f64>>,
        min_periods: Option<usize>,
        center: bool,
    ) -> Self {
        let min_periods = min_periods.unwrap_or(window_size);
        let options = RollingOptionsFixedWindow {
            window_size,
            min_periods,
            weights,
            center,
            fn_params: None,
        };
        self.inner.clone().rolling_median(options).into()
    }

    pub fn rolling_median_by(
        &self,
        by: &RbExpr,
        window_size: String,
        min_periods: usize,
        closed: Wrap<ClosedWindow>,
    ) -> Self {
        let options = RollingOptionsDynamicWindow {
            window_size: Duration::parse(&window_size),
            min_periods,
            closed_window: closed.0,
            fn_params: None,
        };
        self.inner
            .clone()
            .rolling_median_by(by.inner.clone(), options)
            .into()
    }

    pub fn rolling_quantile(
        &self,
        quantile: f64,
        interpolation: Wrap<QuantileInterpolOptions>,
        window_size: usize,
        weights: Option<Vec<f64>>,
        min_periods: Option<usize>,
        center: bool,
    ) -> Self {
        let min_periods = min_periods.unwrap_or(window_size);
        let options = RollingOptionsFixedWindow {
            window_size,
            weights,
            min_periods,
            center,
            fn_params: None,
        };

        self.inner
            .clone()
            .rolling_quantile(interpolation.0, quantile, options)
            .into()
    }

    pub fn rolling_quantile_by(
        &self,
        by: &RbExpr,
        quantile: f64,
        interpolation: Wrap<QuantileInterpolOptions>,
        window_size: String,
        min_periods: usize,
        closed: Wrap<ClosedWindow>,
    ) -> Self {
        let options = RollingOptionsDynamicWindow {
            window_size: Duration::parse(&window_size),
            min_periods,
            closed_window: closed.0,
            fn_params: None,
        };

        self.inner
            .clone()
            .rolling_quantile_by(by.inner.clone(), interpolation.0, quantile, options)
            .into()
    }

    pub fn rolling_skew(&self, window_size: usize, bias: bool) -> Self {
        self.inner.clone().rolling_skew(window_size, bias).into()
    }
}
