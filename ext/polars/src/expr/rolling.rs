use polars::prelude::*;
use std::any::Any;

use crate::conversion::Wrap;
use crate::RbExpr;

impl RbExpr {
    pub fn rolling_sum(
        &self,
        window_size: String,
        weights: Option<Vec<f64>>,
        min_periods: usize,
        center: bool,
        by: Option<String>,
        closed: Option<Wrap<ClosedWindow>>,
    ) -> Self {
        let options = RollingOptions {
            window_size: Duration::parse(&window_size),
            weights,
            min_periods,
            center,
            by,
            closed_window: closed.map(|c| c.0),
            ..Default::default()
        };
        self.inner.clone().rolling_sum(options).into()
    }

    pub fn rolling_min(
        &self,
        window_size: String,
        weights: Option<Vec<f64>>,
        min_periods: usize,
        center: bool,
        by: Option<String>,
        closed: Option<Wrap<ClosedWindow>>,
    ) -> Self {
        let options = RollingOptions {
            window_size: Duration::parse(&window_size),
            weights,
            min_periods,
            center,
            by,
            closed_window: closed.map(|c| c.0),
            ..Default::default()
        };
        self.inner.clone().rolling_min(options).into()
    }

    pub fn rolling_max(
        &self,
        window_size: String,
        weights: Option<Vec<f64>>,
        min_periods: usize,
        center: bool,
        by: Option<String>,
        closed: Option<Wrap<ClosedWindow>>,
    ) -> Self {
        let options = RollingOptions {
            window_size: Duration::parse(&window_size),
            weights,
            min_periods,
            center,
            by,
            closed_window: closed.map(|c| c.0),
            ..Default::default()
        };
        self.inner.clone().rolling_max(options).into()
    }

    pub fn rolling_mean(
        &self,
        window_size: String,
        weights: Option<Vec<f64>>,
        min_periods: usize,
        center: bool,
        by: Option<String>,
        closed: Option<Wrap<ClosedWindow>>,
    ) -> Self {
        let options = RollingOptions {
            window_size: Duration::parse(&window_size),
            weights,
            min_periods,
            center,
            by,
            closed_window: closed.map(|c| c.0),
            ..Default::default()
        };

        self.inner.clone().rolling_mean(options).into()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn rolling_std(
        &self,
        window_size: String,
        weights: Option<Vec<f64>>,
        min_periods: usize,
        center: bool,
        by: Option<String>,
        closed: Option<Wrap<ClosedWindow>>,
        ddof: u8,
    ) -> Self {
        let options = RollingOptions {
            window_size: Duration::parse(&window_size),
            weights,
            min_periods,
            center,
            by,
            closed_window: closed.map(|c| c.0),
            fn_params: Some(Arc::new(RollingVarParams { ddof }) as Arc<dyn Any + Send + Sync>),
        };

        self.inner.clone().rolling_std(options).into()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn rolling_var(
        &self,
        window_size: String,
        weights: Option<Vec<f64>>,
        min_periods: usize,
        center: bool,
        by: Option<String>,
        closed: Option<Wrap<ClosedWindow>>,
        ddof: u8,
    ) -> Self {
        let options = RollingOptions {
            window_size: Duration::parse(&window_size),
            weights,
            min_periods,
            center,
            by,
            closed_window: closed.map(|c| c.0),
            fn_params: Some(Arc::new(RollingVarParams { ddof }) as Arc<dyn Any + Send + Sync>),
        };

        self.inner.clone().rolling_var(options).into()
    }

    pub fn rolling_median(
        &self,
        window_size: String,
        weights: Option<Vec<f64>>,
        min_periods: usize,
        center: bool,
        by: Option<String>,
        closed: Option<Wrap<ClosedWindow>>,
    ) -> Self {
        let options = RollingOptions {
            window_size: Duration::parse(&window_size),
            weights,
            min_periods,
            center,
            by,
            closed_window: closed.map(|c| c.0),
            fn_params: Some(Arc::new(RollingQuantileParams {
                prob: 0.5,
                interpol: QuantileInterpolOptions::Linear,
            }) as Arc<dyn Any + Send + Sync>),
        };
        self.inner.clone().rolling_quantile(options).into()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn rolling_quantile(
        &self,
        quantile: f64,
        interpolation: Wrap<QuantileInterpolOptions>,
        window_size: String,
        weights: Option<Vec<f64>>,
        min_periods: usize,
        center: bool,
        by: Option<String>,
        closed: Option<Wrap<ClosedWindow>>,
    ) -> Self {
        let options = RollingOptions {
            window_size: Duration::parse(&window_size),
            weights,
            min_periods,
            center,
            by,
            closed_window: closed.map(|c| c.0),
            fn_params: Some(Arc::new(RollingQuantileParams {
                prob: quantile,
                interpol: interpolation.0,
            }) as Arc<dyn Any + Send + Sync>),
        };

        self.inner.clone().rolling_quantile(options).into()
    }

    pub fn rolling_skew(&self, window_size: usize, bias: bool) -> Self {
        self.inner.clone().rolling_skew(window_size, bias).into()
    }
}
