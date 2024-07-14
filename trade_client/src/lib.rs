pub mod trade {
    include!("../../proto/generated_code/trade.rs");
}

use pyo3::prelude::*;
<<<<<<< HEAD
use pyo3::wrap_pyfunction;
use pyo3_asyncio::tokio::future_into_py;
=======
use pyo3::conversion::IntoPy;
use pyo3::wrap_pyfunction;
>>>>>>> [misc] native python client has been replaced by rust FFI
use trade::GetAggTradeRequest;
use tonic::transport::Channel;
use trade::trade_client::TradeClient;
use lazy_static::lazy_static;

lazy_static! {
    static ref RUNTIME: tokio::runtime::Runtime = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
}

#[pyclass]
pub struct TradeClientWrapper {
    client: TradeClient<Channel>,
}

#[pymethods]
impl TradeClientWrapper {
    #[new]
    pub fn new(uri: &str) -> Self {
        let channel = RUNTIME.block_on(async move{
            let uri_owned = uri.to_string();  // 将 &str 转换为 String
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
        let resp = self.client.ping(tonic::Request::new(trade::PingRequest{ping: "ping".to_string()}));
        let resp = RUNTIME.block_on(resp);
        Ok(resp.unwrap().into_inner().pong)
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
    Ok(())
}
<<<<<<< HEAD
=======



>>>>>>> [misc] native python client has been replaced by rust FFI
