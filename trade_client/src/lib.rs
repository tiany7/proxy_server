pub mod readers;

use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3::{wrap_pyfunction, conversion::IntoPy, exceptions::PyValueError,types::PyDict };
use readers::readers::{StreamReader, dispatch_readers};
use tonic::transport::Channel;

use crate::readers::trade as trade_pb;
use crate::readers::trade::trade_client::TradeClient;


#[pyclass]
pub struct TradeClientWrapper {
    client: TradeClient<Channel>,
}

#[pymethods]
impl TradeClientWrapper {
    #[new]
    pub fn new(uri: &str) -> Self {
        // get the current runtime
        let channel = pyo3_asyncio::tokio::get_runtime().block_on(async move{
            let uri_owned = uri.to_string();  
            Channel::from_shared(uri_owned)
            .expect("uri is invalid")
            .connect()
            .await
            .expect("channel isn't created sucessfully")
        });
        TradeClientWrapper {
            client: TradeClient::new(channel),
        }
    }

    fn add_async(&mut self, py: Python, a: i32, b: i32) -> PyResult<PyObject> {
        let fut = async move {
            Ok(a + b)
        };
        pyo3_asyncio::tokio::future_into_py(py, fut)
            .map(|py_any| py_any.to_object(py))  // Convert &PyAny to PyObject
    }
    
    // a simple ping message
    fn ping(&mut self) -> PyResult<String> {
        let resp = self.client.ping(tonic::Request::new(trade_pb::PingRequest{ping: "ping".to_string()}));
        let resp = pyo3_asyncio::tokio::get_runtime().block_on(resp);
        Ok(resp.unwrap().into_inner().pong)
    }

    // value must be in secs and dvisible by 60
    fn get_market_data_with_timing(&mut self, symbol: Vec<String>, secs: i64) -> PyResult<PyObject> {
        // check the granularity
        if secs % 60 != 0 {
            return Err(PyValueError::new_err("granularity must be divisible by 60"));
        }
        let symbol_clone = symbol.clone();
        let req = trade_pb::GetMarketDataRequest{
            symbol: symbol[0], 
            granularity: Some(trade_pb::TimeDuration {
                unit: trade_pb::TimeUnit::Seconds as i32,
                value: secs,
            }),
        };
        let fut = self.client.get_market_data_with_timing(tonic::Request::new(req));
        let readers = pyo3_asyncio::tokio::get_runtime().block_on(async move {
            let resp = fut.await.unwrap();
            let resp = resp.into_inner();
            let mut readers = dispatch_readers(symbol_clone, resp).await;
            readers
        });
        Python::with_gil(|py| {
            let dict = PyDict::new(py);
            for (symbol, reader) in readers {
                dict.set_item(symbol, reader).unwrap();
            }
            Ok(dict.into())
        })
    }
}

#[pyfunction]
fn add(a: i32, b: i32) -> PyResult<String> {
    Ok(format!("{}@trade", a + b).to_string())
}

#[pymodule]
fn my_python_package(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(add, m)?)?;
    m.add_class::<TradeClientWrapper>()?;
    m.add_class::<StreamReader>()?;
    Ok(())
}



