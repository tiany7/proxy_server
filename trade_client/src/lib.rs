use std::convert::TryInto;

use pyo3::prelude::*;
use pyo3_asyncio::tokio::future_into_py;
use tonic::transport::Channel;
use tonic::Request;

use trade::trade_client::TradeClient;
use crate::trade::{GetMarketDataRequest, TimeDuration, TimeUnit};

// 包含由 `tonic_build` 编译的 `.proto` 文件
pub mod trade {
    include!("../generated_code/trade.rs");
}

#[pyclass]
struct PyBinanceClient {
    client: TradeClient<Channel>,
}

#[pymethods]
impl PyBinanceClient {
    #[new]
    fn new(py: Python, url: String) -> PyResult<Self> {
        let rt = tokio::runtime::Runtime::new().map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to create runtime: {}", e)))?;
        let client = rt.block_on(async {
            TradeClient::connect(url).await.map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to connect: {}", e)))
        })?;

        Ok(PyBinanceClient { client })
    }

    fn get_market_data<'py>(&self, py: Python<'py>, symbol: String, granularity_value: i64, granularity_unit: i32) -> PyResult<&'py PyAny> {
        let mut client = self.client.clone();
        future_into_py(py, async move {
            let request = Request::new(GetMarketDataRequest {
                symbol,
                granularity: Some(TimeDuration {
                    value: granularity_value,
                    unit: TimeUnit::from_i32(granularity_unit).unwrap().into(),
                }),
            });

            let mut stream = client.get_market_data(request).await.map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to send request: {}", e))
            })?.into_inner();

            let mut market_data = String::new();

            while let Some(response) = stream.message().await.map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to receive response: {}", e))
            })? {
                market_data.push_str(&format!("{:?}\n", response.data));
            }

            Ok(market_data)
        })
    }
}

#[pymodule]
fn trade_client(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyBinanceClient>()?;
    Ok(())
}
